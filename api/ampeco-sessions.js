// api/ampeco-sessions.js
// Vercel Serverless Function (Node runtime)
//
// - JSON for n8n by default
// - ?format=xlsx returns an Excel file with tabs (one per station)
//
// Each station sheet has:
// 1) DETAIL TABLE (columns): id, energy_kwh, startedAt, stoppedAt, tarifas
//    - If no rows -> still outputs 1 row with tarifas="NO SESSIONS"
// 2) MONTH SUMMARY TABLE (same sheet, to the RIGHT, not below):
//    - Month (YYYY-MM), Dieninis_kWh, Naktinis_kWh, Total_kWh, Rows
//
// IMPORTANT FIX for long-running ACTIVE sessions:
// - Your previous code only fetched sessions where startedAt is inside the month.
// - If a session started 5 months ago and is still active, it won’t be returned by filter[startedAfter].
// - So now we ALSO fetch ACTIVE sessions without date filters (best-effort) and then slice their
//   clock-aligned consumption to the requested month range.
//
// Stations include charge point 171: "Aliaksandr Ciurlionio 84A"
//
// ENV VARS:
// - AMPECO_BEARER_TOKEN (required)
// - AMPECO_BASE_URL (optional, default https://cp.ikrautas.lt)
// - INTERNAL_API_KEY (optional; if set, must send header x-api-key or query apiKey)

const XLSX = require("xlsx");

const stations = [
  { chargePointId: 326, stationName: "Vadim Čiurlionio 84A" },
  { chargePointId: 218, stationName: "Ignė Čiurlionio g. 84A" },
  { chargePointId: 27, stationName: "Arnas Čiurlionio 84A" },
  { chargePointId: 171, stationName: "Aliaksandr Ciurlionio 84A" },
];

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function toIsoLocalMonthRangeEuropeVilnius(now = new Date()) {
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
  statusFilter, // optional "active"
}) {
  const url = new URL("/public-api/resources/sessions/v1.0", baseUrl);

  url.searchParams.set("withClockAlignedEnergyConsumption", "true");
  url.searchParams.set("clockAlignedInterval", String(clockAlignedInterval));
  url.searchParams.set("withAuthorization", "true");
  url.searchParams.set("withPriceBreakdown", "true");
  url.searchParams.set("withChargingPeriods", "true");
  url.searchParams.set("withChargingPeriodsPriceBreakdown", "true");

  url.searchParams.set("filter[chargePointId]", String(chargePointId));

  // Only set date filters when provided
  if (startedAfter) url.searchParams.set("filter[startedAfter]", String(startedAfter));
  if (startedBefore) url.searchParams.set("filter[startedBefore]", String(startedBefore));

  // Best-effort: many AMPECO tenants support this
  if (statusFilter) url.searchParams.set("filter[status]", String(statusFilter));

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

async function listPagedSessions({
  baseUrl,
  token,
  chargePointId,
  startedAfter,
  startedBefore,
  clockAlignedInterval,
  perPage = 100,
  statusFilter,
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
      statusFilter,
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

async function listAllSessionsForStationInRange({
  baseUrl,
  token,
  chargePointId,
  startedAfter,
  startedBefore,
  clockAlignedInterval,
  perPage = 100,
}) {
  return listPagedSessions({
    baseUrl,
    token,
    chargePointId,
    startedAfter,
    startedBefore,
    clockAlignedInterval,
    perPage,
  });
}

async function listActiveSessionsForStationBestEffort({
  baseUrl,
  token,
  chargePointId,
  clockAlignedInterval,
  perPage = 100,
}) {
  // Some tenants support filter[status]=active.
  // If not supported, we just return [] and proceed (no hard fail).
  try {
    const active = await listPagedSessions({
      baseUrl,
      token,
      chargePointId,
      startedAfter: null,
      startedBefore: null,
      clockAlignedInterval,
      perPage,
      statusFilter: "active",
    });
    return Array.isArray(active) ? active : [];
  } catch (e) {
    // Don't break the whole export if tenant doesn't support status filter
    return [];
  }
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

      startedAt: s?.startedAt ?? null,
      stoppedAt: s?.stoppedAt ?? null,

      chargingPeriods: Array.isArray(s?.chargingPeriods) ? s.chargingPeriods : [],
      clockAlignedIntervalMinutes: clockAlignedInterval,
      clockAlignedEnergyConsumption: clockAligned,
    };
  });
}

/* -----------------------------
   TARIFAS logic (from your image)
   - Weekend (Sat/Sun): Naktinis all day
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
  return `${p.year}-${p.month}-${p.day}T${String(p.hour).padStart(
    2,
    "0"
  )}:${String(p.minute).padStart(2, "0")}:${String(p.second).padStart(
    2,
    "0"
  )}${off}`;
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

  const wd = weekdayVilnius(d);
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
   Period extraction (DETAIL ROWS)

   Prefer chargingPeriods if available.
   Otherwise, fall back to clockAlignedEnergyConsumption (especially for long-running actives),
   and SLICE to [startedAfter, startedBefore) so a 5-month session shows rows for this month only.
-------------------------------- */

function parseDateSafe(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  return Number.isFinite(d.getTime()) ? d : null;
}

function inRange(d, start, end) {
  if (!d || !start || !end) return false;
  const t = d.getTime();
  return t >= start.getTime() && t < end.getTime();
}

function getClockStartEnd(obj) {
  const start = obj?.start || obj?.startedAt || obj?.from || obj?.periodStart || null;
  const end = obj?.end || obj?.stoppedAt || obj?.to || obj?.periodEnd || null;
  return { start, end };
}

function getClockEnergyWh(obj) {
  // common shapes
  const a =
    safeNum(obj?.energyConsumed) ??
    safeNum(obj?.energyConsumption?.total) ??
    safeNum(obj?.energy) ??
    safeNum(obj?.consumedEnergy) ??
    null;
  return a;
}

function extractDetailRowsForStation({ sessions, startedAfter, startedBefore }) {
  const startD = parseDateSafe(startedAfter);
  const endD = parseDateSafe(startedBefore);

  const rows = [];
  let sumDay = 0;
  let sumNight = 0;
  let rowsCount = 0;

  for (const sess of sessions || []) {
    const sessionId = String(sess.sessionId || "");

    // 1) chargingPeriods (preferred)
    const periods = Array.isArray(sess.chargingPeriods) ? sess.chargingPeriods : [];
    if (periods.length > 0) {
      for (const p of periods) {
        const started = p?.startedAt || p?.start || null;
        const stopped = p?.stoppedAt || p?.end || null;

        // If period has a start time, keep only those starting inside the requested month range
        // (matches how client wants month report)
        const dStart = parseDateSafe(started);
        if (startD && endD && dStart && !inRange(dStart, startD, endD)) continue;

        const energyWh = safeNum(p?.energy) ?? safeNum(p?.energyConsumed) ?? null;
        const energyKwh = energyWh === null ? null : energyWh / 1000;

        const tarifas = tarifasFromVilniusTime(started);

        if (energyKwh !== null) {
          if (tarifas === "Dieninis") sumDay += energyKwh;
          else if (tarifas === "Naktinis") sumNight += energyKwh;
        }

        rowsCount++;

        rows.push({
          id: p?.id ?? "",
          energy_kwh: energyKwh === null ? "" : +energyKwh.toFixed(6),
          startedAt: toVilniusIsoWithOffset(started),
          stoppedAt: toVilniusIsoWithOffset(stopped),
          tarifas,
        });
      }
      continue;
    }

    // 2) fallback: clockAlignedEnergyConsumption (slice to month range)
    const clock = Array.isArray(sess.clockAlignedEnergyConsumption)
      ? sess.clockAlignedEnergyConsumption
      : [];

    if (clock.length > 0) {
      let idx = 0;
      for (const c of clock) {
        const { start, end } = getClockStartEnd(c);
        const dStart = parseDateSafe(start);
        if (startD && endD && dStart && !inRange(dStart, startD, endD)) continue;

        const eWh = getClockEnergyWh(c);
        // keep zeros too if you want, but it bloats; here we keep even 0 (still valid)
        const eKwh = eWh === null ? null : eWh / 1000;

        const tarifas = tarifasFromVilniusTime(start);

        if (eKwh !== null) {
          if (tarifas === "Dieninis") sumDay += eKwh;
          else if (tarifas === "Naktinis") sumNight += eKwh;
        }

        rowsCount++;
        idx++;

        rows.push({
          // clockAligned doesn’t have an id -> create stable id
          id: sessionId ? `${sessionId}_${idx}` : `row_${idx}`,
          energy_kwh: eKwh === null ? "" : +eKwh.toFixed(6),
          startedAt: toVilniusIsoWithOffset(start),
          stoppedAt: toVilniusIsoWithOffset(end),
          tarifas,
        });
      }
    }
  }

  return { rows, sumDay, sumNight, rowsCount };
}

/* -----------------------------
   Excel builder
   - Details in columns A:E
   - Summary in columns G:K (to the right)
-------------------------------- */

function monthKeyFromVilniusIso(isoString) {
  if (!isoString) return "";
  const d = new Date(isoString);
  const p = vilniusParts(d);
  return `${p.year}-${p.month}`; // YYYY-MM
}

function makeStationWorksheet({ stationName, detailRows, summary }) {
  const detailHeader = ["id", "energy_kwh", "startedAt", "stoppedAt", "tarifas"];

  const detailData = detailRows.map((r) => [
    r.id,
    r.energy_kwh,
    r.startedAt,
    r.stoppedAt,
    r.tarifas,
  ]);

  const ws = XLSX.utils.aoa_to_sheet([detailHeader, ...detailData]);

  // Summary to the RIGHT (start at G1)
  const summaryAoa = [
    [`MONTH SUMMARY (${stationName})`],
    ["Month", "Dieninis_kWh", "Naktinis_kWh", "Total_kWh", "Rows"],
    [
      summary.month,
      +summary.dieninis.toFixed(6),
      +summary.naktinis.toFixed(6),
      +(summary.dieninis + summary.naktinis).toFixed(6),
      summary.rows,
    ],
  ];

  XLSX.utils.sheet_add_aoa(ws, summaryAoa, { origin: "G1" });

  // Column widths (A..E) + (G..K)
  ws["!cols"] = [
    { wch: 16 }, // A id
    { wch: 14 }, // B energy_kwh
    { wch: 26 }, // C startedAt
    { wch: 26 }, // D stoppedAt
    { wch: 12 }, // E tarifas
    { wch: 3 },  // F gap
    { wch: 28 }, // G summary title / Month
    { wch: 14 }, // H
    { wch: 14 }, // I
    { wch: 14 }, // J
    { wch: 10 }, // K
  ];

  return ws;
}

function makeExcel({ stationResults, startedAfter, startedBefore }) {
  const wb = XLSX.utils.book_new();
  const month = monthKeyFromVilniusIso(startedAfter);

  for (const st of stationResults) {
    const { rows, sumDay, sumNight, rowsCount } = extractDetailRowsForStation({
      sessions: st.sessions,
      startedAfter,
      startedBefore,
    });

    const detailRows =
      rows.length > 0
        ? rows
        : [
            {
              id: "",
              energy_kwh: "",
              startedAt: "",
              stoppedAt: "",
              tarifas: "NO SESSIONS",
            },
          ];

    const ws = makeStationWorksheet({
      stationName: st.stationName,
      detailRows,
      summary: {
        month,
        dieninis: sumDay,
        naktinis: sumNight,
        rows: rowsCount,
      },
    });

    const sheetName = String(st.stationName).slice(0, 31);
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
      // 1) normal month range sessions (started in range)
      let inRangeSessions = await listAllSessionsForStationInRange({
        baseUrl,
        token,
        chargePointId: station.chargePointId,
        startedAfter,
        startedBefore,
        clockAlignedInterval,
        perPage,
      });

      // 2) ALSO fetch active sessions (might have started months ago)
      let activeSessions = await listActiveSessionsForStationBestEffort({
        baseUrl,
        token,
        chargePointId: station.chargePointId,
        clockAlignedInterval,
        perPage,
      });

      // Merge by session id (avoid duplicates)
      const byId = new Map();
      for (const s of [...(inRangeSessions || []), ...(activeSessions || [])]) {
        if (!s) continue;
        const id = String(s.id ?? "");
        if (!id) continue;
        if (!byId.has(id)) byId.set(id, s);
      }

      let mergedSessions = Array.from(byId.values());

      // Enrich actives with consumption stats clockAligned (best-effort)
      mergedSessions = await enrichActiveSessionsConsumptionStats({
        baseUrl,
        token,
        sessions: mergedSessions,
        clockAlignedInterval,
      });

      const normalized = normalizeSessionsForN8n({
        station,
        sessions: mergedSessions,
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
      const buf = makeExcel({ stationResults, startedAfter, startedBefore });

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
