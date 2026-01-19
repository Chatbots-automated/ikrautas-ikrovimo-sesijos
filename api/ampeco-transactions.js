// Vercel Serverless Function (plain)
// File: /api/ampeco-transactions.js
//
// GET /api/ampeco-transactions?createdAfter=...&createdBefore=...&per_page=100&max_pages=200&max_items=20000
//
// What it does (matches your n8n flow):
// 1) Fetch ALL transactions for date range (cursor pagination)
// 2) Filter:
//    - totalAmount != 0
//    - status === "finalized"
//    - paymentMethod matches: "<any letters> **** <4 digits>" (e.g. "mastercard **** 4263")
// 3) For each remaining transaction, GET /users/{userId}/invoice-details
// 4) Keep only items where invoice-details.requireInvoice === false
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

    // limit invoice-details calls (concurrency)
    const concurrency = clampInt(req.query.concurrency, 1, 25, 10);

    // regex like your n8n filter:
    // "mastercard **** 4263" => letters + spaces + **** + spaces + 4 digits
    // You can override via ?paymentRegex=...
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
      `&cursor`; // empty cursor engages cursor pagination

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

    // ---------- 2) Apply your Filter node logic ----------
    const filtered = transactions.filter((t) => {
      const totalAmount = Number(t?.totalAmount ?? t?.amount ?? 0);
      const status = String(t?.status ?? "");
      const paymentMethod = String(t?.paymentMethod ?? "");

      if (!Number.isFinite(totalAmount) || totalAmount === 0) return false;
      if (status !== "finalized") return false;
      if (!paymentRegex.test(paymentMethod)) return false;

      return true;
    });

    // ---------- 3) Fetch invoice-details per userId (cached) ----------
    const cache = new Map(); // userId -> invoiceDetails|null
    const missingUserId = { count: 0 };
    const invoiceFetchErrors = { count: 0 };

    const tasks = filtered.map((t) => async () => {
      const userId = t?.userId;
      if (userId == null) {
        missingUserId.count++;
        return { t, invoiceDetails: null };
      }

      const key = String(userId);
      if (cache.has(key)) {
        return { t, invoiceDetails: cache.get(key) };
      }

      const url = `${baseUrl}/public-api/resources/users/v1.0/${encodeURIComponent(
        key
      )}/invoice-details`;

      try {
        const details = await fetchJson(url, token);
        cache.set(key, details);
        return { t, invoiceDetails: details };
      } catch (e) {
        invoiceFetchErrors.count++;
        cache.set(key, null); // avoid retrying 10x for same user
        return { t, invoiceDetails: null, invoiceDetailsError: e?.message || String(e) };
      }
    });

    const results = await runWithConcurrency(tasks, concurrency);

    // ---------- 4) Filter1: requireInvoice === false ----------
    const final = results
      .filter((x) => x?.invoiceDetails && x.invoiceDetails.requireInvoice === false)
      .map((x) => ({
        // keep transaction fields you care about (you can add more)
        transactionId: x.t?.id ?? null,
        userId: x.t?.userId ?? null,
        status: x.t?.status ?? null,
        totalAmount: x.t?.totalAmount ?? null,
        paymentMethod: x.t?.paymentMethod ?? null,
        createdAt: x.t?.createdAt ?? x.t?.created_at ?? null,

        // invoice-details (what your next nodes would need)
        requireInvoice: x.invoiceDetails?.requireInvoice ?? null,
        invoiceDetails: x.invoiceDetails,
      }));

    return res.status(200).json({
      ok: true,
      createdAfter,
      createdBefore,
      per_page: perPage,
      pagesFetched,
      fetchedCount: transactions.length,

      filteredCount: filtered.length,
      uniqueUsersChecked: cache.size,
      requireInvoiceFalseCount: final.length,

      debug: {
        paymentRegex: paymentRegex.toString(),
        missingUserId: missingUserId.count,
        invoiceFetchErrors: invoiceFetchErrors.count,
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

async function fetchJson(url, token) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25000);

  try {
    // basic retry for 429/5xx
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

      // retryable?
      const retryable = r.status === 429 || (r.status >= 500 && r.status <= 599);
      if (!retryable || attempt === 4) {
        throw new Error(`HTTP ${r.status} ${r.statusText}: ${text.slice(0, 500)}`);
      }

      // small backoff
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
        out[idx] = { error: e?.message || String(e) };
      }
    }
  });

  await Promise.all(workers);
  return out;
}
