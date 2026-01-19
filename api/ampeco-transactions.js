// Vercel Serverless Function (plain)
// File: /api/ampeco-transactions.js
//
// GET /api/ampeco-transactions?createdAfter=...&createdBefore=...&per_page=100&max_pages=200&max_items=20000
//
// What it does (matches your n8n flow, but returns UNIQUE clients with email):
// 1) Fetch ALL transactions for date range (cursor pagination)
// 2) Filter:
//    - totalAmount != 0
//    - status === "finalized"
//    - paymentMethod matches: "<any letters> **** <4 digits>" (e.g. "mastercard **** 4263")
// 3) Deduplicate by userId (so 10 tx for same user -> 1 result)
// 4) For each unique userId, GET /users/{userId}/invoice-details
// 5) Keep only users where invoice-details.requireInvoice === false
// 6) Ensure email is included:
//    - Try invoice-details fields first
//    - If missing, fallback to GET /users/{userId} (cached)
// 7) Response: data = [{ userId, email, invoiceDetails, sampleTransaction, ... }]
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

    // ---------- query params ----------
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

    // concurrency for API calls
    const concurrency = clampInt(req.query.concurrency, 1, 25, 10);

    // payment method regex (your n8n logic)
    const paymentRegexStr =
      typeof req.query.paymentRegex === "string" && req.query.paymentRegex.trim()
        ? req.query.paymentRegex.trim()
        : String.raw`[A-Za-z].*\*{4}\s*\d{4}\b`;

    let paymentRegex;
    try {
      paymentRegex = new RegExp(paymentRegexStr);
    } catch {
      paymentRegex = /[A-Za-z].*\*{4}\s*\d{4}\b/;
    }

    // ---------- 1) Fetch all transactions (cursor pagination) ----------
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

    const transactions = all.slice(0, maxItems);

    // ---------- 2) Filter like your n8n Filter node ----------
    const filtered = transactions.filter((t) => {
      const totalAmount = Number(t?.totalAmount ?? t?.amount ?? 0);
      const status = String(t?.status ?? "");
      const paymentMethod = String(t?.paymentMethod ?? "");

      if (!Number.isFinite(totalAmount) || totalAmount === 0) return false;
      if (status !== "finalized") return false;
      if (!paymentRegex.test(paymentMethod)) return false;

      return true;
    });

    // ---------- 3) Deduplicate by userId (unique clients only) ----------
    // Keep the FIRST transaction we see per userId as "sampleTransaction"
    const byUser = new Map(); // userId -> { userId, sampleTransaction }
    let missingUserIdCount = 0;

    for (const t of filtered) {
      const userId = t?.userId;
      if (userId == null) {
        missingUserIdCount++;
        continue;
      }
      const key = String(userId);
      if (!byUser.has(key)) {
        byUser.set(key, { userId: key, sampleTransaction: t });
      }
    }

    const uniqueUsers = Array.from(byUser.values());

    // ---------- 4/5/6) invoice-details + email enrichment (cached) ----------
    const invoiceCache = new Map(); // userId -> invoiceDetails|null
    const userCache = new Map(); // userId -> userProfile|null
    const invoiceFetchErrors = { count: 0 };
    const userFetchErrors = { count: 0 };

    const tasks = uniqueUsers.map((u) => async () => {
      const userId = u.userId;

      // ----- invoice-details -----
      let invoiceDetails = invoiceCache.get(userId);
      if (invoiceDetails === undefined) {
        const url = `${baseUrl}/public-api/resources/users/v1.0/${encodeURIComponent(
          userId
        )}/invoice-details`;
        try {
          invoiceDetails = await fetchJson(url, token);
          invoiceCache.set(userId, invoiceDetails);
        } catch (e) {
          invoiceFetchErrors.count++;
          invoiceDetails = null;
          invoiceCache.set(userId, null);
        }
      }

      // Filter1: requireInvoice === false
      const requireInvoice = invoiceDetails?.requireInvoice;
      if (requireInvoice !== false) {
        return null; // exclude
      }

      // ----- email (best effort) -----
      // invoice-details email field names can vary, so try multiple
      let email =
        pickFirstString(
          invoiceDetails?.email,
          invoiceDetails?.userEmail,
          invoiceDetails?.contactEmail,
          invoiceDetails?.billingEmail
        ) || null;

      // fallback: fetch user profile if email missing
      if (!email) {
        let profile = userCache.get(userId);
        if (profile === undefined) {
          const url = `${baseUrl}/public-api/resources/users/v1.0/${encodeURIComponent(
            userId
          )}`;
          try {
            profile = await fetchJson(url, token);
            userCache.set(userId, profile);
          } catch {
            userFetchErrors.count++;
            profile = null;
            userCache.set(userId, null);
          }
        }

        // common shapes: { email } or { data: { email } }
        email =
          pickFirstString(profile?.email, profile?.data?.email, profile?.user?.email) ||
          null;
      }

      return {
        userId,
        email,
        requireInvoice: false,
        invoiceDetails,
        sampleTransaction: u.sampleTransaction,
      };
    });

    const enriched = await runWithConcurrency(tasks, concurrency);
    const final = enriched.filter(Boolean);

    return res.status(200).json({
      ok: true,
      createdAfter,
      createdBefore,
      per_page: perPage,
      pagesFetched,

      fetchedCount: transactions.length,
      filteredCount: filtered.length,
      uniqueUsersAfterFilter: uniqueUsers.length,
      requireInvoiceFalseCount: final.length,

      debug: {
        paymentRegex: paymentRegex.toString(),
        missingUserIdCount,
        invoiceDetailsFetchErrors: invoiceFetchErrors.count,
        userProfileFetchErrors: userFetchErrors.count,
      },

      data: final,
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

function pickFirstString(...vals) {
  for (const v of vals) {
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return null;
}

async function fetchJson(url, token) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25000);

  try {
    for (let attempt = 1; attempt <= 4; attempt++) {
      const r = await fetch(url, {
        method: "GET",
        headers: {
          accept: "application/json",
          authorization: `Bearer ${token}`,
        },
        signal: controller.signal,
      });

      const text = await r.text();

      if (r.ok) {
        try {
          return JSON.parse(text);
        } catch {
          throw new Error(`Non-JSON response: ${text.slice(0, 500)}`);
        }
      }

      const retryable = r.status === 429 || (r.status >= 500 && r.status <= 599);
      if (!retryable || attempt === 4) {
        throw new Error(`HTTP ${r.status} ${r.statusText}: ${text.slice(0, 500)}`);
      }

      await sleep(300 * attempt);
    }

    throw new Error("Unexpected fetchJson fallthrough");
  } finally {
    clearTimeout(timeout);
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function runWithConcurrency(tasks, limit) {
  const out = [];
  let i = 0;

  const workers = Array.from({ length: limit }, async () => {
    while (i < tasks.length) {
      const idx = i++;
      try {
        out[idx] = await tasks[idx]();
      } catch (e) {
        out[idx] = null;
      }
    }
  });

  await Promise.all(workers);
  return out;
}
