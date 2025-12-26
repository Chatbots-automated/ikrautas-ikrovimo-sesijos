// api/ampeco-sessions.js
// Vercel Serverless Function (Node runtime)
// - JSON for n8n by default
// - ?format=xlsx returns an Excel file with 3 tabs (one per station)

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
  // NOTE: we output WITHOUT timezone suffix; AMPECO accepts ISO strings either way,
  // but you can pass exact strings via query params to override.
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

function whToKwh(wh) {
  const n = safeNum(wh);
  return n === null ? null : n / 1000;
}

function buildSessionsUrl({
  baseUrl,
  token,
  chargePointId,
  startedAfter,
  startedBefore,
  clockAlignedInterval,
  cursor,
  perPage,
}) {
  const url = new URL("/public-api/resources/sessions/v1.0", baseUrl);

  // Expansions/flags (matching your curl)
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

// Best-effort (docs page is hard to fetch via this environment, but endpoint naming follows AMPECO conventions)
// If it 404s on your tenant, we just skip enrichment and keep listing payload.
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
  } catch (e) {
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
      token,
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
  // Only enrich actives (and also if listing returned a suspiciously large clockAligned array)
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

      // We don’t know the exact response shape on your tenant/version,
      // so we try common patterns:
      // - { data: [...] }
      // - { clockAlignedEnergyConsumption: [...] }
      // - { data: { clockAlignedEnergyConsumption: [...] } }
      const clock =
        (Array.isArray(stats?.data) && stats.data) ||
        (Array.isArray(stats?.clockAlignedEnergyConsumption) &&
          stats.clockAlignedEnergyConsumption) ||
        (Array.isArray(stats?.data?.clockAlignedEnergyConsumption) &&
          stats.data.clockAlignedEnergyConsumption) ||
        null;

      if (clock) {
        enriched.push({ ...s, _consumptionStatsClockAligned: clock });
      } else {
        enriched.push(s);
      }
    } catch (e) {
      // If the endpoint is not available or errors → keep original session
      enriched.push(s);
    }
  }
  return enriched;
}

function normalizeSessionsForN8n({ station, sessions, clockAlignedInterval }) {
  return sessions.map((s) => {
    const clockFromListing = Array.isArray(s?.clockAlignedEnergyConsumption)
      ? s.clockAlignedEnergyConsumption
      : [];

    const clockFromStats = Array.isArray(s?._consumptionStatsClockAligned)
      ? s._consumptionStatsClockAligned
      : [];

    // Prefer consumption-stats clockAligned if present (especially for active)
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

      evseId: s?.evseId ?? null,
      evsePhysicalReference: s?.evsePhysicalReference ?? null,

      authorizationId: s?.authorizationId ?? null,
      authorization: s?.authorization ?? null,

      idTag: s?.idTag ?? null,
      idTagLabel: s?.idTagLabel ?? null,

      energyWh: safeNum(s?.energy),
      energyKwh: whToKwh(s?.energy),

      energyConsumptionTotalWh: safeNum(s?.energyConsumption?.total),
      energyConsumptionGridWh: safeNum(s?.energyConsumption?.grid),

      totalAmountWithTax: safeNum(s?.totalAmount?.withTax),
      totalAmountWithoutTax: safeNum(s?.totalAmount?.withoutTax),
      currency: s?.currency ?? null,

      billingStatus: s?.billingStatus ?? null,
      paymentType: s?.paymentType ?? null,
      paymentMethodId: s?.paymentMethodId ?? null,

      powerLatest: safeNum(s?.power?.latest),
      powerPeak: safeNum(s?.power?.peak),
      powerAverage: safeNum(s?.power?.average),

      chargingPeriods: Array.isArray(s?.chargingPeriods) ? s.chargingPeriods : [],
      clockAlignedIntervalMinutes: clockAlignedInterval,
      clockAlignedEnergyConsumption: clockAligned,
    };
  });
}

function makeExcel({ stationNormalized }) {
  // 3 tabs: one per station
  // Each sheet contains CLOCK-ALIGNED rows (most useful for reporting)
  const wb = XLSX.utils.book_new();

  for (const st of stationNormalized) {
    const rows = [];

    for (const sess of st.sessions) {
      const clock = Array.isArray(sess.clockAlignedEnergyConsumption)
        ? sess.clockAlignedEnergyConsumption
        : [];

      if (clock.length === 0) {
        rows.push({
          stationName: sess.stationName,
          chargePointId: sess.chargePointId,
          sessionId: sess.sessionId,
          status: sess.status,
          startedAt: sess.startedAt,
          stoppedAt: sess.stoppedAt,
          periodStart: null,
          periodEnd: null,
          energyConsumedWh: null,
          energyConsumedKwh: null,
          gridWh: null,
          gridKwh: null,
          totalCostWithTax: null,
          totalCostWithoutTax: null,
          sessionEnergyWh: sess.energyWh,
          sessionEnergyKwh: sess.energyKwh,
          sessionTotalWithTax: sess.totalAmountWithTax,
          sessionTotalWithoutTax: sess.totalAmountWithoutTax,
          currency: sess.currency,
        });
        continue;
      }

      for (const p of clock) {
        const eWh =
          safeNum(p?.energyConsumed) ??
          safeNum(p?.energyConsumption?.total) ??
          null;
        const gWh = safeNum(p?.energyConsumption?.grid) ?? null;

        rows.push({
          stationName: sess.stationName,
          chargePointId: sess.chargePointId,
          sessionId: sess.sessionId,
          status: sess.status,
          startedAt: sess.startedAt,
          stoppedAt: sess.stoppedAt,
          periodStart: p?.start ?? null,
          periodEnd: p?.end ?? null,
          energyConsumedWh: eWh,
          energyConsumedKwh: eWh === null ? null : eWh / 1000,
          gridWh: gWh,
          gridKwh: gWh === null ? null : gWh / 1000,
          totalCostWithTax: safeNum(p?.totalCost?.withTax),
          totalCostWithoutTax: safeNum(p?.totalCost?.withoutTax),
          sessionEnergyWh: sess.energyWh,
          sessionEnergyKwh: sess.energyKwh,
          sessionTotalWithTax: sess.totalAmountWithTax,
          sessionTotalWithoutTax: sess.totalAmountWithoutTax,
          currency: sess.currency,
        });
      }
    }

    const sheetName = String(st.stationName).slice(0, 31); // Excel sheet name limit
    const ws = XLSX.utils.json_to_sheet(rows);
    XLSX.utils.book_append_sheet(wb, ws, sheetName);
  }

  return XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
}

module.exports = async (req, res) => {
  try {
    // Optional internal protection (recommended)
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

    const perPage = Math.min(
      100,
      Math.max(1, Number(req.query.per_page || 100))
    );

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

    if (totalSessions === 0) {
      // Must explicitly say no sessions
      const payload = {
        ok: true,
        noSessions: true,
        message: "There were no sessions in the selected period.",
        generatedAt: new Date().toISOString(),
        range: { startedAfter, startedBefore, clockAlignedInterval },
        stations: stationResults,
      };

      if (format === "xlsx") {
        // Generate a minimal Excel file too
        const wb = XLSX.utils.book_new();
        const ws = XLSX.utils.json_to_sheet([
          {
            message: payload.message,
            startedAfter,
            startedBefore,
            clockAlignedInterval,
          },
        ]);
        XLSX.utils.book_append_sheet(wb, ws, "Info");
        const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });

        res.setHeader(
          "Content-Type",
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        );
        res.setHeader(
          "Content-Disposition",
          `attachment; filename="ampeco_sessions_none.xlsx"`
        );
        return res.status(200).send(buf);
      }

      return res.status(200).json(payload);
    }

    // Combined flat arrays (handy for n8n)
    const flatSessions = stationResults.flatMap((s) => s.sessions);
    const flatClock = [];
    for (const s of flatSessions) {
      const clock = Array.isArray(s.clockAlignedEnergyConsumption)
        ? s.clockAlignedEnergyConsumption
        : [];
      for (const p of clock) {
        flatClock.push({
          stationName: s.stationName,
          chargePointId: s.chargePointId,
          sessionId: s.sessionId,
          status: s.status,
          startedAt: s.startedAt,
          stoppedAt: s.stoppedAt,
          periodStart: p?.start ?? null,
          periodEnd: p?.end ?? null,
          energyConsumedWh:
            safeNum(p?.energyConsumed) ??
            safeNum(p?.energyConsumption?.total) ??
            null,
          gridWh: safeNum(p?.energyConsumption?.grid) ?? null,
          totalCostWithTax: safeNum(p?.totalCost?.withTax),
          totalCostWithoutTax: safeNum(p?.totalCost?.withoutTax),
        });
      }
    }

    const payload = {
      ok: true,
      noSessions: false,
      message: "Sessions fetched successfully.",
      generatedAt: new Date().toISOString(),
      range: { startedAfter, startedBefore, clockAlignedInterval },
      totals: {
        sessions: totalSessions,
        clockAlignedRows: flatClock.length,
      },
      stations: stationResults,
      flatSessions,
      flatClockAligned: flatClock,
    };

    if (format === "xlsx") {
      const buf = makeExcel({ stationNormalized: stationResults });

      res.setHeader(
        "Content-Type",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      );
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="ampeco_sessions_${startedAfter.replace(
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
