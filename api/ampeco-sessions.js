// /api/ampeco-sessions.js
// Node runtime (Vercel Serverless Function)
//
// GET /api/ampeco-sessions?startedAfter=2025-11-01T00:00:00&startedBefore=2026-01-01T00:00:00&includeExcel=1
//
// Env:
//  - AMPECO_BASE_URL=https://cp.ikrautas.lt
//  - AMPECO_BEARER_TOKEN=xxxx
//  - AMPECO_CHARGEPOINT_IDS=27,28,29
//
// Notes:
//  - Cursor pagination: per_page + cursor=null first page, then cursor=meta.next_cursor :contentReference[oaicite:2]{index=2}

const ExcelJS = require("exceljs");

const DEFAULT_FLAGS = {
  withClockAlignedEnergyConsumption: "true",
  clockAlignedInterval: "15",
  withAuthorization: "true",
  withPriceBreakdown: "true",
  withChargingPeriods: "true",
  withChargingPeriodsPriceBreakdown: "true",
};

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function parseChargePointIds() {
  const raw = mustEnv("AMPECO_CHARGEPOINT_IDS");
  const ids = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((x) => Number(x))
    .filter((n) => Number.isFinite(n));

  if (ids.length !== 3) {
    throw new Error(
      `AMPECO_CHARGEPOINT_IDS must contain exactly 3 numeric ids (got: "${raw}")`
    );
  }
  return ids;
}

function buildSessionsUrl({ baseUrl, chargePointId, startedAfter, startedBefore, perPage, cursor, page }) {
  const url = new URL("/public-api/resources/sessions/v1.0", baseUrl);

  // your flags
  for (const [k, v] of Object.entries(DEFAULT_FLAGS)) url.searchParams.set(k, v);

  // filters (same style as your curl)
  url.searchParams.set("filter[chargePointId]", String(chargePointId));
  url.searchParams.set("filter[startedAfter]", startedAfter);
  url.searchParams.set("filter[startedBefore]", startedBefore);

  // pagination
  url.searchParams.set("per_page", String(perPage));
  url.searchParams.set("page", String(page));
  url.searchParams.set("cursor", cursor === null ? "null" : String(cursor));

  return url.toString();
}

async function fetchJsonWithRetry(url, { headers, timeoutMs = 30000, retries = 4 } = {}) {
  let lastErr = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);

    try {
      const res = await fetch(url, { headers, signal: ctrl.signal });
      clearTimeout(t);

      if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
        // retryable
        const txt = await res.text().catch(() => "");
        lastErr = new Error(`HTTP ${res.status} retryable. Body: ${txt.slice(0, 500)}`);
      } else if (!res.ok) {
        const txt = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status}. Body: ${txt.slice(0, 2000)}`);
      } else {
        return await res.json();
      }
    } catch (e) {
      clearTimeout(t);
      lastErr = e;
    }

    // backoff
    const delay = Math.min(2000 * Math.pow(2, attempt), 12000);
    await new Promise((r) => setTimeout(r, delay));
  }

  throw lastErr || new Error("Unknown fetch error");
}

async function fetchAllSessionsForChargePoint({ baseUrl, token, chargePointId, startedAfter, startedBefore }) {
  const perPage = 100; // max per docs :contentReference[oaicite:3]{index=3}
  let cursor = null;   // first page: cursor=null :contentReference[oaicite:4]{index=4}
  let page = 1;

  const all = [];

  while (true) {
    const url = buildSessionsUrl({
      baseUrl,
      chargePointId,
      startedAfter,
      startedBefore,
      perPage,
      cursor,
      page,
    });

    const payload = await fetchJsonWithRetry(url, {
      headers: {
        accept: "application/json",
        authorization: `Bearer ${token}`,
      },
    });

    const data = Array.isArray(payload?.data) ? payload.data : [];
    all.push(...data);

    const nextCursor = payload?.meta?.next_cursor ?? null; // docs :contentReference[oaicite:5]{index=5}
    if (!nextCursor) break;

    cursor = nextCursor;
    page += 1;
  }

  return all;
}

function toNumberOrNull(x) {
  if (x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function flattenRowsForExcel(sessions, chargePointId) {
  const rows = [];

  for (const s of sessions) {
    const sessionId = s?.id ?? null;
    const status = s?.status ?? null;
    const startedAt = s?.startedAt ?? null;
    const stoppedAt = s?.stoppedAt ?? null;

    const totalWithTax = s?.totalAmount?.withTax ?? null;
    const totalWithoutTax = s?.totalAmount?.withoutTax ?? null;

    const energyTotal = s?.energyConsumption?.total ?? s?.energy ?? null;
    const energyGrid = s?.energyConsumption?.grid ?? null;

    const evsePhysicalReference = s?.evsePhysicalReference ?? null;
    const authorizationMethod = s?.authorization?.method ?? null;

    const clock = Array.isArray(s?.clockAlignedEnergyConsumption)
      ? s.clockAlignedEnergyConsumption
      : [];

    if (clock.length === 0) {
      // still output the session (so you see it in the report)
      rows.push({
        chargePointId,
        sessionId,
        status,
        startedAt,
        stoppedAt,
        evsePhysicalReference,
        authorizationMethod,
        intervalStart: null,
        intervalEnd: null,
        intervalEnergyConsumed: null,
        intervalEnergyTotal: null,
        intervalEnergyGrid: null,
        intervalTotalCostWithTax: null,
        intervalTotalCostWithoutTax: null,
        sessionTotalWithTax: totalWithTax,
        sessionTotalWithoutTax: totalWithoutTax,
        sessionEnergyTotal: energyTotal,
        sessionEnergyGrid: energyGrid,
      });
      continue;
    }

    for (const p of clock) {
      rows.push({
        chargePointId,
        sessionId,
        status,
        startedAt,
        stoppedAt,
        evsePhysicalReference,
        authorizationMethod,
        intervalStart: p?.start ?? null,
        intervalEnd: p?.end ?? null,
        intervalEnergyConsumed: toNumberOrNull(p?.energyConsumed),
        intervalEnergyTotal: toNumberOrNull(p?.energyConsumption?.total ?? p?.energyConsumption?.grid ?? null),
        intervalEnergyGrid: toNumberOrNull(p?.energyConsumption?.grid ?? null),
        intervalTotalCostWithTax: toNumberOrNull(p?.totalCost?.withTax ?? p?.totalCost?.with_tax ?? null),
        intervalTotalCostWithoutTax: toNumberOrNull(p?.totalCost?.withoutTax ?? p?.totalCost?.without_tax ?? null),
        sessionTotalWithTax: totalWithTax,
        sessionTotalWithoutTax: totalWithoutTax,
        sessionEnergyTotal: energyTotal,
        sessionEnergyGrid: energyGrid,
      });
    }
  }

  return rows;
}

async function buildWorkbookBase64({ stationRows, stationOrder }) {
  const wb = new ExcelJS.Workbook();
  wb.creator = "Vercel AMPECO Export";

  const columns = [
    { header: "chargePointId", key: "chargePointId", width: 14 },
    { header: "sessionId", key: "sessionId", width: 12 },
    { header: "status", key: "status", width: 12 },
    { header: "startedAt", key: "startedAt", width: 24 },
    { header: "stoppedAt", key: "stoppedAt", width: 24 },
    { header: "evsePhysicalReference", key: "evsePhysicalReference", width: 18 },
    { header: "authorizationMethod", key: "authorizationMethod", width: 18 },

    { header: "intervalStart", key: "intervalStart", width: 24 },
    { header: "intervalEnd", key: "intervalEnd", width: 24 },
    { header: "intervalEnergyConsumed", key: "intervalEnergyConsumed", width: 20 },
    { header: "intervalEnergyTotal", key: "intervalEnergyTotal", width: 18 },
    { header: "intervalEnergyGrid", key: "intervalEnergyGrid", width: 18 },
    { header: "intervalTotalCostWithTax", key: "intervalTotalCostWithTax", width: 20 },
    { header: "intervalTotalCostWithoutTax", key: "intervalTotalCostWithoutTax", width: 24 },

    { header: "sessionTotalWithTax", key: "sessionTotalWithTax", width: 18 },
    { header: "sessionTotalWithoutTax", key: "sessionTotalWithoutTax", width: 22 },
    { header: "sessionEnergyTotal", key: "sessionEnergyTotal", width: 18 },
    { header: "sessionEnergyGrid", key: "sessionEnergyGrid", width: 18 },
  ];

  for (const cpId of stationOrder) {
    const ws = wb.addWorksheet(`CP_${cpId}`.slice(0, 31)); // Excel tab name max 31 chars
    ws.columns = columns;

    const rows = stationRows[String(cpId)] || [];
    for (const r of rows) ws.addRow(r);

    ws.getRow(1).font = { bold: true };
    ws.views = [{ state: "frozen", ySplit: 1 }];
  }

  const buf = await wb.xlsx.writeBuffer();
  return Buffer.from(buf).toString("base64");
}

module.exports = async (req, res) => {
  try {
    if (req.method !== "GET") {
      res.status(405).json({ ok: false, error: "Method not allowed. Use GET." });
      return;
    }

    const baseUrl = process.env.AMPECO_BASE_URL || "https://cp.ikrautas.lt";
    const token = mustEnv("AMPECO_BEARER_TOKEN");
    const chargePointIds = parseChargePointIds();

    const startedAfter = req.query.startedAfter;
    const startedBefore = req.query.startedBefore;
    const includeExcel = String(req.query.includeExcel || "") === "1";

    if (!startedAfter || !startedBefore) {
      res.status(400).json({
        ok: false,
        error:
          "Missing required query params: startedAfter, startedBefore (ISO datetime strings). Example: ?startedAfter=2025-11-01T00:00:00&startedBefore=2026-01-01T00:00:00",
      });
      return;
    }

    // fetch all 3 in parallel (fast)
    const results = await Promise.all(
      chargePointIds.map(async (cpId) => {
        const sessions = await fetchAllSessionsForChargePoint({
          baseUrl,
          token,
          chargePointId: cpId,
          startedAfter,
          startedBefore,
        });

        const rows = flattenRowsForExcel(sessions, cpId);
        return { chargePointId: cpId, sessions, rows };
      })
    );

    const totalSessions = results.reduce((sum, r) => sum + r.sessions.length, 0);
    const stationRows = {};
    const stations = results.map((r) => {
      stationRows[String(r.chargePointId)] = r.rows;
      return {
        chargePointId: r.chargePointId,
        sessionsCount: r.sessions.length,
        rowsCount: r.rows.length,
        sessions: r.sessions, // raw sessions (with clockAlignedEnergyConsumption etc.)
      };
    });

    if (totalSessions === 0) {
      res.status(200).json({
        ok: true,
        message: "No sessions in the given period.",
        startedAfter,
        startedBefore,
        stations: stations.map((s) => ({ ...s, sessions: [] })),
      });
      return;
    }

    let excel = null;
    if (includeExcel) {
      const filename = `ampeco_sessions_${startedAfter.replace(/[:+]/g, "-")}_to_${startedBefore.replace(
        /[:+]/g,
        "-"
      )}.xlsx`;

      const base64 = await buildWorkbookBase64({
        stationRows,
        stationOrder: chargePointIds,
      });

      excel = {
        filename,
        mimeType:
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        base64,
      };
    }

    // n8n-friendly structured output
    res.status(200).json({
      ok: true,
      startedAfter,
      startedBefore,
      totalSessions,
      chargePointIds,
      stations,
      excel,
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err?.message || String(err),
    });
  }
};
