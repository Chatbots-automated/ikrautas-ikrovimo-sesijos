// Vercel (Next.js) Serverless Function
// File: /api/ampeco-transactions.js
//
// GET /api/ampeco-transactions?createdAfter=...&createdBefore=...&per_page=100
// - Uses cursor pagination automatically (follows response.links.next)
// - Accepts date filters (ISO 8601 strings)
// - Returns ALL transactions for that range in one response
//
// ENV REQUIRED:
// AMPECO_BASE_URL = https://cp.ikrautas.lt
// AMPECO_TOKEN    = <Bearer token>

module.exports = async function handler(req, res) {
  try {
    if (req.method !== "GET") {
      res.setHeader("Allow", "GET");
      return res.status(405).json({ error: "Method not allowed" });
    }

    const baseUrl = (process.env.AMPECO_BASE_URL || "").replace(/\/$/, "");
    const token = process.env.AMPECO_TOKEN;

    if (!baseUrl || !token) {
      return res.status(500).json({
        error: "Missing env vars",
        missing: {
          AMPECO_BASE_URL: !baseUrl,
          AMPECO_TOKEN: !token,
        },
      });
    }

    // --- query params ---
    const perPage = clampInt(req.query.per_page, 1, 100, 100);

    // createdAfter / createdBefore (ISO8601). If not provided, default to current month [start, nextMonthStart)
    const now = new Date();
    const defaultCreatedAfter = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1, 0, 0, 0)).toISOString();
    const defaultCreatedBefore = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1, 0, 0, 0)).toISOString();

    const createdAfter = toIsoOrDefault(req.query.createdAfter, defaultCreatedAfter);
    const createdBefore = toIsoOrDefault(req.query.createdBefore, defaultCreatedBefore);

    // safety caps (avoid infinite loops / huge payloads)
    const maxPages = clampInt(req.query.max_pages, 1, 200, 200);
    const maxItems = clampInt(req.query.max_items, 1, 20000, 20000);

    // --- build first URL (cursor pagination: include cursor param empty) ---
    const firstUrl =
      `${baseUrl}/public-api/resources/transactions/v1.0` +
      `?filter[createdAfter]=${encodeURIComponent(createdAfter)}` +
      `&filter[createdBefore]=${encodeURIComponent(createdBefore)}` +
      `&per_page=${encodeURIComponent(String(perPage))}` +
      `&cursor`; // empty cursor engages cursor pagination

    const all = [];
    let url = firstUrl;
    let pageCount = 0;

    while (url && pageCount < maxPages && all.length < maxItems) {
      pageCount++;

      const json = await fetchJson(url, token);

      // Most Ampeco endpoints are shaped like { data: [...], links: { next: ... }, meta: {...} }
      const rows = Array.isArray(json?.data) ? json.data : [];
      all.push(...rows);

      // Prefer links.next. Fallback to meta.cursor if they ever return cursor token only.
      const next =
        (json?.links && typeof json.links.next === "string" && json.links.next) ||
        null;

      url = next;

      // hard stop if server returns no next + no rows (done)
      if (!url) break;
    }

    // Trim if exceeded maxItems
    const data = all.slice(0, maxItems);

    return res.status(200).json({
      ok: true,
      count: data.length,
      per_page: perPage,
      createdAfter,
      createdBefore,
      pagesFetched: pageCount,
      truncated: all.length > maxItems,
      data,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err?.message || String(err),
    });
  }
}

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

    // If some WAF/ddos page ever appears, you'll see it immediately
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
