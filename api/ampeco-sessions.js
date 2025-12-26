// api/ampeco-sessions.js
// Vercel Serverless Function (Node runtime)
// - JSON for n8n by default
// - ?format=xlsx returns an Excel file with 3 tabs (one per station)
// Excel tabs contain ONLY: id, energy_kwh, startedAt, stoppedAt, tarifas
// If a station has no sessions/periods in the month -> still outputs 1 row with tarifas="NO SESSIONS"

const XLSX = require("xlsx");

const stations = [
  { chargePointId: 326, stationName: "Vadim Čiurlionio 84A" },
  { chargePointId: 218, stationName: "Ignė Čiurlionio g. 84A" },
  { chargePointId: 27, stationName: "Arnas Čiurlionio 84A" },
];

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function toIsoLocalMonthRangeEuropeVilnius(now = new Date()) {
  // Default range: current month in Europe/Vilnius
  // startedAfter = first day 00:00:00
  // startedBefore = first day of next month 00:00:00
  const tz = "Europe/Vilnius";
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);

  const year = parts.find((p) => p.type === "year").value;
  const month = parts.find((p) => p.type === "month").value;

  const y = Number(year);
  const m = Number(month);

  const pad2 = (n) => String(n).padStart(2, "0");

  const startedAfter = `${year}-${month}-01T00:00:00`;
  const nextY = m === 12 ? y + 1 : y;
  const nextM = m === 12 ? 1 : m + 1;
  const startedBefore = `${nextY}-${pad2(nextM)}-01T00:00:00`;

  return { startedAfter, startedBefore };
}

function safeNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function buildSessionsUrl({
  baseUrl,
  chargePointId,
  startedAfter,
  startedBefore,
  clockAlignedInterval,
  cursor,
  perPage,
}) {
  const url = new URL("/public-api/resources/sessions/v1.0", baseUrl);

  // Expansions/flags
  url.searchParams.set("withClockAlignedEnergyConsumption", "true");
  url.searchParams.set("clockAlignedInterval", String(clockAlignedInterval));
  url.searchParams.set("withAuthorization", "true");
  url.searchParams.set("withPriceBreakdown", "true");
  url.searchParams.set("withChargingPeriods", "true");
  url.searchParams.set("withChargingPeriodsPriceBreakdown", "true");

  // Filters
  url.searchParams.set("filter[chargePointId]", String(chargePointId));
  url.searchParams.set("filter[startedAfter]", startedAfter);
  url.searchParams.set("filter[startedBefore]", startedBefore);

  // Pagination
  url.searchParams.set("per_page", String(perPage));
  url.searchParams.set("cursor", cursor === null ? "null" : String(cursor));

  return url.toString();
}

// Best-effort enrichment endpoint (if it 404s, we ignore it)
function buildConsumptionStatsUrl({ baseUrl, sessionId, clockAlignedInterval }) {
  const url = new URL(
    `/public-api/resources/sessions/v1.0/${encodeURIComponent(
      String(sessionId)
    )}/consumption-stats`,
    baseUrl
  );
  url.searchParams.set("clockAlignedInterval", String(clockAlignedInterval));
  return url.toString();
}

async function ampecoGetJson(url, token) {
  const r = await fetch(url, {
    method: "GET",
    headers: {
      accept: "application/json",
      authorization: `Bearer ${token}`,
    },
  });

  const text = await r.text();
  let json;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { _raw: text };
  }

  if (!r.ok) {
    const msg =
      json && typeof json === "object"
        ? JSON.stringify(json)
        : String(text || "");
    const err = new Error(`AMPECO ${r.status} ${r.statusText}: ${msg}`);
    err.status = r.status;
    throw err;
  }

  return json;
}

async function listAllSessionsForStation({
  baseUrl,
  token,
  chargePointId,
  startedAfter,
  startedBefore,
  clockAlignedInterval,
  perPage = 100,
}) {
  let cursor = null;
  const all = [];

  while (true) {
    const url = buildSessionsUrl({
      baseUrl,
      chargePointId,
      startedAfter,
      startedBefore,
      clockAlignedInterval,
      cursor,
      perPage,
    });

    const resp = await ampecoGetJson(url, token);

    const data = Array.isArray(resp?.data) ? resp.data : [];
    all.push(...data);

    const nextCursor = resp?.meta?.next_cursor ?? null;
    if (!nextCursor) break;
    cursor = nextCursor;
  }

  return all;
}

async function enrichActiveSessionsConsumptionStats({
  baseUrl,
  token,
  sessions,
  clockAlignedInterval,
}) {
  const enriched = [];
  for (const s of sessions) {
    const status = String(s?.status || "");
    const listingClock = Array.isArray(s?.clockAlignedEnergyConsumption)
      ? s.clockAlignedEnergyConsumption
      : [];

    const shouldTry =
      status === "active" || (listingClock && listingClock.length >= 300);

    if (!shouldTry) {
      enriched.push(s);
      continue;
    }

    try {
      const url = buildConsumptionStatsUrl({
        baseUrl,
        sessionId: s.id,
        clockAlignedInterval,
      });
      const stats = await ampecoGetJson(url, token);

      const clock =
        (Array.isArray(stats?.data) && stats.data) ||
        (Array.isArray(stats?.clockAlignedEnergyConsumption) &&
          stats.clockAlignedEnergyConsumption) ||
        (Array.isArray(stats?.data?.clockAlignedEnergyConsumption) &&
          stats.data.clockAlignedEnergyConsumption) ||
        null;

      if (clock) enriched.push({ ...s, _consumptionStatsClockAligned: clock });
      else enriched.push(s);
    } catch {
      enriched.push(s);
    }
  }
  return enriched;
}

function normalizeSessionsForN8n({ station, sessions, clockAlignedInterval }) {
  return (sessions || []).map((s) => {
    const clockFromListing = Array.isArray(s?.clockAlignedEnergyConsumption)
      ? s.clockAlignedEnergyConsumption
      : [];
    const clockFromStats = Array.isArray(s?._consumptionStatsClockAligned)
      ? s._consumptionStatsClockAligned
      : [];

    const clockAligned =
      clockFromStats.length > 0 ? clockFromStats : clockFromListing;

    return {
      stationName: station.stationName,
      chargePointId: station.chargePointId,

      sessionId: String(s?.id ?? ""),
      status: s?.status ?? null,
      reason: s?.reason ?? null,

      startedAt: s?.startedAt ?? null,
      stoppedAt: s?.stoppedAt ?? null,
      lastUpdatedAt: s?.lastUpdatedAt ?? null,

      // Keep these for JSON/debug/optional usage
      energyWh: safeNum(s?.energy),
      energyConsumptionTotalWh: safeNum(s?.energyConsumption?.total),
      energyConsumptionGridWh: safeNum(s?.energyConsumption?.grid),

      chargingPeriods: Array.isArray(s?.chargingPeriods) ? s.chargingPeriods : [],
      clockAlignedIntervalMinutes: clockAlignedInterval,
      clockAlignedEnergyConsumption: clockAligned,
    };
  });
}

/* -----------------------------
   TARIFAS logic (from your image)
   - Weekend (Sat/Sun) always Naktinis
   - Weekdays:
     Summer time: Dieninis 08:00–24:00, Naktinis 00:00–08:00
     Winter time: Dieninis 07:00–23:00, Naktinis 23:00–07:00
   Detected by Vilnius timezone offset (+03 summer, +02 winter)
-------------------------------- */

const VILNIUS_TZ = "Europe/Vilnius";

function vilniusParts(date) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: VILNIUS_TZ,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZoneName: "shortOffset", // "GMT+2"/"GMT+3"
  }).formatToParts(date);

  const get = (t) => parts.find((p) => p.type === t)?.value;

  return {
    year: get("year"),
    month: get("month"),
    day: get("day"),
    hour: Number(get("hour")),
    minute: Number(get("minute")),
    second: Number(get("second")),
    tzOffset: get("timeZoneName") || "GMT+0",
  };
}

function offsetToHHMM(tzOffsetStr) {
  const m = String(tzOffsetStr).match(/GMT([+-])(\d{1,2})(?::(\d{2}))?/);
  if (!m) return "+00:00";
  const sign = m[1];
  const hh = String(m[2]).padStart(2, "0");
  const mm = String(m[3] || "00").padStart(2, "0");
  return `${sign}${hh}:${mm}`;
}

function toVilniusIsoWithOffset(isoString) {
  if (!isoString) return "";
  const d = new Date(isoString);
  const p = vilniusParts(d);
  const off = offsetToHHMM(p.tzOffset);
  return `${p.year}-${p.month}-${p.day}T${String(p.hour).padStart(2, "0")}:${String(
    p.minute
  ).padStart(2, "0")}:${String(p.second).padStart(2, "0")}${off}`;
}

function weekdayVilnius(date) {
  const w = new Intl.DateTimeFormat("en-US", {
    timeZone: VILNIUS_TZ,
    weekday: "short",
  }).format(date);
  const map = { Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6, Sun: 7 };
  return map[w] || 0;
}

function isSummerTimeVilnius(date) {
  const p = vilniusParts(date);
  const off = offsetToHHMM(p.tzOffset);
  return off === "+03:00";
}

function tarifasFromVilniusTime(isoString) {
  if (!isoString) return "";
  const d = new Date(isoString);

  const wd = weekdayVilnius(d); // 1..7
  if (wd === 6 || wd === 7) return "Naktinis"; // VI–VII

  const p = vilniusParts(d);
  const t = p.hour * 60 + p.minute;

  const summer = isSummerTimeVilnius(d);
  const dayStart = (summer ? 8 : 7) * 60;
  const dayEnd = (summer ? 24 : 23) * 60;

  const isDay = t >= dayStart && t < Math.min(dayEnd, 1440);
  return isDay ? "Dieninis" : "Naktinis";
}

/* -----------------------------
   Excel builder (chargingPeriods only)
   Columns: id, energy_kwh, startedAt, stoppedAt, tarifas
   If station has no periods -> 1 row with tarifas="NO SESSIONS"
-------------------------------- */

function makeExcelPeriodsOnly({ stationNormalized }) {
  const wb = XLSX.utils.book_new();

  for (const st of stationNormalized) {
    const rows = [];

    for (const sess of st.sessions || []) {
      const periods = Array.isArray(sess.chargingPeriods) ? sess.chargingPeriods : [];
      for (const p of periods) {
        const energyWh = Number(p?.energy ?? 0);
        rows.push({
          id: p?.id ?? "",
          energy_kwh: Number.isFinite(energyWh) ? +(energyWh / 1000).toFixed(6) : "",
          startedAt: toVilniusIsoWithOffset(p?.startedAt),
          stoppedAt: toVilniusIsoWithOffset(p?.stoppedAt),
          tarifas: tarifasFromVilniusTime(p?.startedAt),
        });
      }
    }

    if (rows.length === 0) {
      rows.push({
        id: "",
        energy_kwh: "",
        startedAt: "",
        stoppedAt: "",
        tarifas: "NO SESSIONS",
      });
    }

    const sheetName = String(st.stationName).slice(0, 31);
    const ws = XLSX.utils.json_to_sheet(rows);
    XLSX.utils.book_append_sheet(wb, ws, sheetName);
  }

  return XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
}

module.exports = async (req, res) => {
  try {
    // Optional internal protection
    const internalKey = process.env.INTERNAL_API_KEY;
    if (internalKey) {
      const got =
        req.headers["x-api-key"] ||
        req.headers["X-Api-Key"] ||
        req.query.apiKey;
      if (String(got || "") !== String(internalKey)) {
        return res.status(401).json({ error: "Unauthorized" });
      }
    }

    const baseUrl = (process.env.AMPECO_BASE_URL || "https://cp.ikrautas.lt")
      .trim()
      .replace(/\/$/, "");
    const token = requireEnv("AMPECO_BEARER_TOKEN");

    const { startedAfter: defAfter, startedBefore: defBefore } =
      toIsoLocalMonthRangeEuropeVilnius(new Date());

    const startedAfter = String(req.query.startedAfter || defAfter);
    const startedBefore = String(req.query.startedBefore || defBefore);

    const clockAlignedInterval = Number(req.query.clockAlignedInterval || 15);
    const format = String(req.query.format || "json").toLowerCase();

    const perPage = Math.min(100, Math.max(1, Number(req.query.per_page || 100)));

    const stationResults = [];
    let totalSessions = 0;

    for (const station of stations) {
      let sessions = await listAllSessionsForStation({
        baseUrl,
        token,
        chargePointId: station.chargePointId,
        startedAfter,
        startedBefore,
        clockAlignedInterval,
        perPage,
      });

      sessions = await enrichActiveSessionsConsumptionStats({
        baseUrl,
        token,
        sessions,
        clockAlignedInterval,
      });

      const normalized = normalizeSessionsForN8n({
        station,
        sessions,
        clockAlignedInterval,
      });

      totalSessions += normalized.length;

      stationResults.push({
        stationName: station.stationName,
        chargePointId: station.chargePointId,
        sessionsCount: normalized.length,
        sessions: normalized,
      });
    }

    // JSON payload for n8n (keep it structured + include periods)
    const payload = {
      ok: true,
      generatedAt: new Date().toISOString(),
      range: { startedAfter, startedBefore, clockAlignedInterval },
      totals: { sessions: totalSessions },
      stations: stationResults,
      noSessions: totalSessions === 0,
      message:
        totalSessions === 0
          ? "There were no sessions in the selected period."
          : "Sessions fetched successfully.",
    };

    if (format === "xlsx") {
      const buf = makeExcelPeriodsOnly({ stationNormalized: stationResults });

      res.setHeader(
        "Content-Type",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      );
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="ampeco_periods_${startedAfter.replace(
          /[:+]/g,
          "-"
        )}_${startedBefore.replace(/[:+]/g, "-")}.xlsx"`
      );
      return res.status(200).send(buf);
    }

    return res.status(200).json(payload);
  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: e?.message || String(e),
    });
  }
};
