// Vercel Serverless Function (plain)
// File: /api/ampeco-transactions.js
//
// GET /api/ampeco-transactions?createdAfter=...&createdBefore=...&per_page=100&max_pages=200&max_items=20000
//
// Matches your n8n flow BUT does it efficiently:
//
// 1) Fetch ALL transactions for date range (cursor pagination)
// 2) Filter transactions:
//    - totalAmount != 0
//    - status === "finalized"
//    - paymentMethod matches: "<any letters> **** <4 digits>"
// 3) For unique userIds from filtered transactions:
//    - GET /users/{userId}/invoice-details (cached)
//    - Keep only users where requireInvoice === false
//    - Extract email (invoice-details -> fallback to /users/{userId})
// 4) Return ALL filtered transactions whose userId is allowlisted
//    + add userEmail per transaction
//    + add transactionDate (finalizedAt -> createdAt -> lastUpdatedAt)
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

    const now = new Date();
    const defaultCreatedAfter = new Date(
      Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1, 0, 0, 0)
    ).toISOString();
    const defaultCreatedBefore = new Date(
      Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1, 0, 0, 0)
    ).toISOString();

    const createdAfter = toIsoOrDefault(req.query.createdAfter, defaultCreatedAfter);
    const createdBefore = toIsoOrDefault(req.query.createdBefore, defaultCreatedBefore);

    const maxPages = clampInt(req.query.max_pages, 1, 500, 200);
    const maxItems = clampInt(req.query.max_items, 1, 100000, 20000);
    const concurrency = clampInt(req.query.concurrency, 1, 25, 10);

    // payment method regex (same intent as your n8n regex)
    const paymentRegexStr =
      typeof req.query.paymentRegex === "string" && req.query.paymentRegex.trim()
        ? req.query.paymentRegex.trim()
        : String.raw`.*\*{4}\s*\d{4}\b`;

    let paymentRegex;
    try {
      paymentRegex = new RegExp(paymentRegexStr);
    } catch {
      paymentRegex = /.*\*{4}\s*\d{4}\b/;
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

      // cursor pagination: usually links.next
      nextUrl =
        (json?.links && typeof json.links.next === "string" && json.links.next) ||
        (json?.links && typeof json.links.next_url === "string" && json.links.next_url) ||
        (typeof json?.next_url === "string" && json.next_url) ||
        null;

      if (!nextUrl) break;
    }

    const transactions = all.slice(0, maxItems);

    // ---------- 2) Filter transactions like your n8n Filter node ----------
    const reasons = { totalZero: 0, statusNotFinal: 0, payNoMatch: 0, missingUserId: 0 };
    const filtered = [];

    for (const t of transactions) {
      const totalAmount = Number(t?.totalAmount ?? t?.amount ?? 0);
      const status = String(t?.status ?? "");
      const paymentMethod = String(t?.paymentMethod ?? "");
      const userId = t?.userId;

      if (!Number.isFinite(totalAmount) || totalAmount === 0) {
        reasons.totalZero++;
        continue;
      }
      if (status !== "finalized") {
        reasons.statusNotFinal++;
        continue;
      }
      if (!paymentRegex.test(paymentMethod)) {
        reasons.payNoMatch++;
        continue;
      }
      if (userId == null) {
        reasons.missingUserId++;
        continue;
      }

      filtered.push(t);
    }

    // ---------- 3) Build unique userId set from filtered transactions ----------
    const userIds = [...new Set(filtered.map((t) => String(t.userId)))];

    // caches
    const invoiceCache = new Map(); // userId -> invoiceDetails|null
    const userCache = new Map(); // userId -> userProfile|null

    let invoiceFetchErrors = 0;
    let userFetchErrors = 0;

    // fetch invoice-details for each unique userId (concurrently)
    const userTasks = userIds.map((userId) => async () => {
      // invoice-details
      let invoiceDetails = invoiceCache.get(userId);
      if (invoiceDetails === undefined) {
        const url = `${baseUrl}/public-api/resources/users/v1.0/${encodeURIComponent(
          userId
        )}/invoice-details`;
        try {
          invoiceDetails = await fetchJson(url, token);
          invoiceCache.set(userId, invoiceDetails);
        } catch {
          invoiceFetchErrors++;
          invoiceDetails = null;
          invoiceCache.set(userId, null);
        }
      }

      const requireInvoice = invoiceDetails?.requireInvoice;

      // only allow requireInvoice === false (same as your Filter1)
      if (requireInvoice !== false) {
        return { userId, allowed: false, email: null, invoiceDetails };
      }

      // email from invoiceDetails (try several keys)
      let email =
        pickFirstString(
          invoiceDetails?.email,
          invoiceDetails?.userEmail,
          invoiceDetails?.contactEmail,
          invoiceDetails?.billingEmail
        ) || null;

      // fallback to user profile endpoint if still missing
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
            userFetchErrors++;
            profile = null;
            userCache.set(userId, null);
          }
        }

        email =
          pickFirstString(profile?.email, profile?.data?.email, profile?.user?.email) ||
          null;
      }

      return { userId, allowed: true, email, invoiceDetails };
    });

    const userResults = await runWithConcurrency(userTasks, concurrency);

    // allowlist of userIds
    const allowedUsers = new Map(); // userId -> { email, invoiceDetails }
    for (const r of userResults) {
      if (r && r.allowed) {
        allowedUsers.set(String(r.userId), {
          email: r.email || null,
          invoiceDetails: r.invoiceDetails || null,
        });
      }
    }

    // ---------- 4) Return ALL filtered transactions whose user is allowlisted ----------
    const finalTransactions = filtered
      .filter((t) => allowedUsers.has(String(t.userId)))
      .map((t) => {
        const u = allowedUsers.get(String(t.userId));

        const transactionDate = pickFirstString(
          t?.finalizedAt,
          t?.finalized_at,
          t?.createdAt,
          t?.created_at,
          t?.lastUpdatedAt,
          t?.last_updated_at,
          t?.updatedAt,
          t?.updated_at
        );

        return {
          // transaction fields
          transactionId: t?.id ?? null,
          userId: t?.userId ?? null,
          status: t?.status ?? null,
          totalAmount: t?.totalAmount ?? null,
          paymentMethod: t?.paymentMethod ?? null,

          // ✅ the date you want
          transactionDate: transactionDate ?? null,

          // keep raw timestamps too (helpful for debugging)
          createdAt: t?.createdAt ?? t?.created_at ?? null,
          finalizedAt: t?.finalizedAt ?? t?.finalized_at ?? null,
          lastUpdatedAt: t?.lastUpdatedAt ?? t?.last_updated_at ?? null,

          // fields your n8n needs
          userEmail: u?.email ?? null,
          requireInvoice: false,
          invoiceDetails: u?.invoiceDetails ?? null,
        };
      });

    return res.status(200).json({
      ok: true,
      createdAfter,
      createdBefore,
      per_page: perPage,
      pagesFetched,

      fetchedCount: transactions.length,
      filteredCount: filtered.length,

      uniqueUsersAfterFilter: userIds.length,
      allowlistedUsers: allowedUsers.size,

      requireInvoiceFalseTransactionCount: finalTransactions.length,

      debug: {
        paymentRegex: paymentRegex.toString(),
        filterDropReasons: reasons,
        invoiceDetailsFetchErrors: invoiceFetchErrors,
        userProfileFetchErrors: userFetchErrors,

        // if you suspect “too little”, this tells you if you hit caps
        caps: {
          maxPages,
          maxItems,
          hitMaxPages: pagesFetched >= maxPages,
          hitMaxItems: all.length >= maxItems,
        },
      },

      data: finalTransactions,
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
      } catch {
        out[idx] = null;
      }
    }
  });

  await Promise.all(workers);
  return out;
}
