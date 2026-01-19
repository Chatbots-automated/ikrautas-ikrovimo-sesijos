// Vercel Serverless Function (plain)
// File: api/ampeco-transactions.js
//
// GET /api/ampeco-transactions?createdAfter=...&createdBefore=...&per_page=100&max_pages=200&max_items=20000
//
// ENV REQUIRED:
// AMPECO_BASE_URL = https://cp.ikrautas.lt
// AMPECO_TOKEN    = <Bearer token>

module.exports = async (req, res) => {
  try {
    if (req.method !== "GET") {
      res.setHeader("Allow", "GET");
      return res.status(405).json({ ok: false, error: "Method not allowed" });
    }

    const baseUrl = String(process.env.AMPECO_BASE_URL || "").replace(/\/$/, "");
    const token = process.env.AMPECO_TOKEN;

    if (!baseUrl || !token) {
      return res.status(500).json({
        ok: false,
        error: "Missing env vars",
        missing: {
          AMPECO_BASE_URL: !baseUrl,
          AMPECO_TOKEN: !token,
        },
      });
    }

    const perPage = clampInt(req.query.per_page, 1, 100, 100);

    // Defaults: current month [start, nextMonthStart)
    const now = new Date();
    const defaultCreatedAfter = new Date(
      Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1, 0, 0, 0)
    ).toISOString();
    const defaultCreatedBefore = new Date(
      Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1, 0, 0, 0)
    ).toISOString();

    const createdAfter = toIsoOrDefault(req.query.createdAfter, defaultCreatedAfter);
    const createdBefore = toIsoOrDefault(req.query.createdBefore, defaultCreatedBefore);

    // safety caps
    const maxPages = clampInt(req.query.max_pages, 1, 500, 200);
    const maxItems = clampInt(req.query.max_items, 1, 100000, 20000);

    // Engage cursor pagination by including empty cursor param
    const firstUrl =
      `${baseUrl}/public-api/resources/transactions/v1.0` +
      `?filter[createdAfter]=${encodeURIComponent(createdAfter)}` +
      `&filter[createdBefore]=${encodeURIComponent(createdBefore)}` +
      `&per_page=${encodeURIComponent(String(perPage))}` +
      `&cursor`;

    let nextUrl = firstUrl;
    let pagesFetched = 0;
    const all = [];

    while (nextUrl && pagesFetched < maxPages && all.length < maxItems) {
      pagesFetched++;

      const json = await fetchJson(nextUrl, token);

      const rows = Array.isArray(json?.data) ? json.data : [];
      all.push(...rows);

      nextUrl =
        (json?.links && typeof json.links.next === "string" && json.links.next) || null;

      if (!nextUrl) break;
    }

    const data = all.slice(0, maxItems);

    return res.status(200).json({
      ok: true,
      count: data.length,
      per_page: perPage,
      createdAfter,
      createdBefore,
      pagesFetched,
      truncated: all.length > maxItems,
      data,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err?.message || String(err),
    });
  }
};

// ---------- helpers ----------

function clampInt(v, min, max, def) {
  const n = Number(v);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, Math.trunc(n)));
}

function toIsoOrDefault(v, def) {
  if (!v) return def;
  const s = String(v).trim();
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return def;
  return d.toISOString();
}

async function fetchJson(url, token) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25000);

  try {
    const r = await fetch(url, {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${token}`,
      },
      signal: controller.signal,
    });

    const text = await r.text();

    if (!r.ok) {
      throw new Error(`HTTP ${r.status} ${r.statusText}: ${text.slice(0, 500)}`);
    }

    try {
      return JSON.parse(text);
    } catch {
      throw new Error(`Non-JSON response: ${text.slice(0, 500)}`);
    }
  } finally {
    clearTimeout(timeout);
  }
}
