// Vercel Serverless Function (plain)
// File: /api/ampeco-transactions.js
//
// GET /api/ampeco-transactions?createdAfter=...&createdBefore=...&per_page=100&max_pages=200&max_items=20000
//
// ✅ Robust "fetch ALL" logic:
// - Handles relative links.next
// - Prevents cursor loops
// - Multi-pass sweep to catch missing records when dataset changes mid-pagination
// - Fixes AbortController retry bug (per-attempt controller)
//
// ✅ SessionId support remains the same:
//   includeSession=1
//   details_concurrency=10
//   max_details=5000
//
// ✅ LT time (Europe/Vilnius, GMT+2/+3 DST):
// - Response adds LT versions of all timestamps:
//   transactionDateLt, createdAtLt, finalizedAtLt, lastUpdatedAtLt
// - Also if you omit createdAfter/createdBefore, defaults are computed in LT time
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

    // Defaults computed in LT time (Europe/Vilnius), then converted to ISO (UTC) for API filters
    const now = new Date();
    const ltNowParts = getTimeZoneParts(now, "Europe/Vilnius");
    const ltYear = ltNowParts.year;
    const ltMonth = ltNowParts.month; // 1-12

    const defaultCreatedAfter = makeLtLocalIsoUtc(ltYear, ltMonth, 1, 0, 0, 0); // LT start of month
    const defaultCreatedBefore = makeLtLocalIsoUtc(
      ltMonth === 12 ? ltYear + 1 : ltYear,
      ltMonth === 12 ? 1 : ltMonth + 1,
      1,
      0,
      0,
      0
    ); // LT start of next month

    const createdAfter = toIsoOrDefault(req.query.createdAfter, defaultCreatedAfter);
    const createdBefore = toIsoOrDefault(req.query.createdBefore, defaultCreatedBefore);

    // hard caps (still respected)
    const maxPages = clampInt(req.query.max_pages, 1, 2000, 200);
    const maxItems = clampInt(req.query.max_items, 1, 200000, 20000);

    const concurrency = clampInt(req.query.concurrency, 1, 25, 10);

    const includeSession =
      String(req.query.includeSession ?? req.query.include_session ?? "0") === "1";

    const detailsConcurrency = clampInt(req.query.details_concurrency, 1, 25, 10);
    const maxDetails = clampInt(req.query.max_details, 1, 200000, 5000);

    // How many full sweeps to do to avoid "missing 5-10" due to live dataset.
    const sweepPasses = clampInt(req.query.sweep_passes, 1, 5, 3);

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

    // ---------- 1) Fetch ALL transactions (cursor pagination, sweep) ----------
    const firstUrl =
      `${baseUrl}/public-api/resources/transactions/v1.0` +
      `?filter[createdAfter]=${encodeURIComponent(createdAfter)}` +
      `&filter[createdBefore]=${encodeURIComponent(createdBefore)}` +
      `&per_page=${encodeURIComponent(String(perPage))}` +
      `&cursor`;

    const fetchDebug = {
      pagesFetchedTotal: 0,
      passes: [],
      hitMaxPages: false,
      hitMaxItems: false,
      loopBreaks: 0,
      tz: "Europe/Vilnius",
    };

    // Dedupe across sweeps by transaction id
    const byId = new Map(); // id -> transactionRow

    for (let pass = 1; pass <= sweepPasses; pass++) {
      let nextUrl = firstUrl;
      let pagesFetchedThisPass = 0;
      let addedThisPass = 0;

      // Avoid infinite loops if API returns same next cursor again
      const seenNextUrls = new Set();

      while (nextUrl && pagesFetchedThisPass < maxPages && byId.size < maxItems) {
        pagesFetchedThisPass++;
        fetchDebug.pagesFetchedTotal++;

        const loopKey = String(nextUrl);
        if (seenNextUrls.has(loopKey)) {
          fetchDebug.loopBreaks++;
          break;
        }
        seenNextUrls.add(loopKey);

        const json = await fetchJson(nextUrl, token);

        const rows = Array.isArray(json?.data) ? json.data : [];
        for (const r of rows) {
          const id = r?.id;
          if (id == null) continue;
          const key = String(id);
          if (!byId.has(key)) {
            byId.set(key, r);
            addedThisPass++;
            if (byId.size >= maxItems) break;
          }
        }

        const rawNext =
          (json?.links && typeof json.links.next === "string" && json.links.next) ||
          (json?.links && typeof json.links.next_url === "string" && json.links.next_url) ||
          (typeof json?.next_url === "string" && json.next_url) ||
          null;

        nextUrl = normalizeNextUrl(rawNext, baseUrl);
        if (!nextUrl) break;
      }

      if (pagesFetchedThisPass >= maxPages) fetchDebug.hitMaxPages = true;
      if (byId.size >= maxItems) fetchDebug.hitMaxItems = true;

      fetchDebug.passes.push({
        pass,
        pagesFetchedThisPass,
        addedThisPass,
        totalUniqueSoFar: byId.size,
      });

      // If a pass added 0 new ids, we’re stable -> stop early
      if (addedThisPass === 0) break;
    }

    const transactions = Array.from(byId.values());

    // ---------- 2) Filter transactions (unchanged) ----------
    const reasons = {
      totalZero: 0,
      statusNotFinal: 0,
      payNoMatch: 0,
      missingUserId: 0,
    };
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

    // ---------- 3) Build unique userId set ----------
    const userIds = [...new Set(filtered.map((t) => String(t.userId)))];

    const invoiceCache = new Map(); // userId -> invoiceDetails|null
    const userCache = new Map(); // userId -> userProfile|null

    let invoiceFetchErrors = 0;
    let userFetchErrors = 0;

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

      if (requireInvoice !== false) {
        return { userId, allowed: false, email: null, invoiceDetails };
      }

      let email =
        pickFirstString(
          invoiceDetails?.email,
          invoiceDetails?.userEmail,
          invoiceDetails?.contactEmail,
          invoiceDetails?.billingEmail
        ) || null;

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

    const allowedUsers = new Map();
    for (const r of userResults) {
      if (r && r.allowed) {
        allowedUsers.set(String(r.userId), {
          email: r.email || null,
          invoiceDetails: r.invoiceDetails || null,
        });
      }
    }

    // ---------- 4) Return ALL filtered transactions whose user is allowlisted ----------
    let finalTransactions = filtered
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
          t?.updated_at,
          t?.date
        );

        const createdAt = t?.createdAt ?? t?.created_at ?? null;
        const finalizedAt = t?.finalizedAt ?? t?.finalized_at ?? null;
        const lastUpdatedAt = t?.lastUpdatedAt ?? t?.last_updated_at ?? null;

        return {
          transactionId: t?.id ?? null,
          userId: t?.userId ?? null,
          status: t?.status ?? null,
          totalAmount: t?.totalAmount ?? null,
          paymentMethod: t?.paymentMethod ?? null,

          // UTC/offset timestamps (as received)
          transactionDate: transactionDate ?? null,
          createdAt,
          finalizedAt,
          lastUpdatedAt,

          // ✅ LT time ISO strings (Europe/Vilnius)
          transactionDateLt: toLtIso(transactionDate),
          createdAtLt: toLtIso(createdAt),
          finalizedAtLt: toLtIso(finalizedAt),
          lastUpdatedAtLt: toLtIso(lastUpdatedAt),

          userEmail: u?.email ?? null,
          requireInvoice: false,
          invoiceDetails: u?.invoiceDetails ?? null,
        };
      });

    // ---------- 5) OPTIONAL: Fetch transaction details to get sessionId ----------
    let sessionFetchErrors = 0;
    let sessionFetched = 0;
    const detailsCache = new Map();

    if (includeSession) {
      const capped = finalTransactions.slice(0, maxDetails);

      const detailTasks = capped.map((t) => async () => {
        const txId = t?.transactionId;
        if (txId == null) return { txId: null, details: null };

        const key = String(txId);
        if (detailsCache.has(key)) return { txId: key, details: detailsCache.get(key) };

        const url = `${baseUrl}/public-api/resources/transactions/v1.0/${encodeURIComponent(
          key
        )}`;

        try {
          const details = await fetchJson(url, token);
          detailsCache.set(key, details);
          sessionFetched++;
          return { txId: key, details };
        } catch {
          sessionFetchErrors++;
          detailsCache.set(key, null);
          return { txId: key, details: null };
        }
      });

      const detailResults = await runWithConcurrency(detailTasks, detailsConcurrency);

      const byTxId = new Map();
      for (const r of detailResults) {
        if (!r?.txId) continue;
        byTxId.set(String(r.txId), r.details);
      }

      finalTransactions = finalTransactions.map((t) => {
        const txId = t?.transactionId != null ? String(t.transactionId) : null;
        const d = txId ? byTxId.get(txId) : null;

        const sessionId = d?.sessionId ?? d?.session_id ?? null;

        const betterDate = pickFirstString(
          d?.finalizedAt,
          d?.date,
          d?.lastUpdatedAt,
          d?.createdAt
        );

        const betterDateLt = toLtIso(betterDate) ?? t.transactionDateLt ?? null;

        return {
          ...t,
          sessionId,
          txNumber: d?.number ?? null,
          ref: d?.ref ?? null,
          purchaseResourceType: d?.purchaseResourceType ?? null,
          purchaseResourceId: d?.purchaseResourceId ?? null,

          transactionDate: betterDate ?? t.transactionDate ?? null,
          transactionDateLt: betterDateLt,

          // also provide LT versions if detail endpoint returned better raw fields
          finalizedAtLt: toLtIso(d?.finalizedAt) ?? t.finalizedAtLt ?? null,
          createdAtLt: toLtIso(d?.createdAt) ?? t.createdAtLt ?? null,
          lastUpdatedAtLt: toLtIso(d?.lastUpdatedAt) ?? t.lastUpdatedAtLt ?? null,
        };
      });
    }

    return res.status(200).json({
      ok: true,
      createdAfter,
      createdBefore,
      per_page: perPage,

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

        includeSession,
        sessionFetched,
        sessionFetchErrors,
        detailsConcurrency,
        maxDetails,

        fetchAll: fetchDebug,

        // LT info
        timeZone: "Europe/Vilnius",

        caps: {
          maxPages,
          maxItems,
          hitMaxPages: fetchDebug.hitMaxPages,
          hitMaxItems: fetchDebug.hitMaxItems,
          hitMaxDetails: includeSession && finalTransactions.length > maxDetails,
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

function normalizeNextUrl(nextUrl, baseUrl) {
  if (!nextUrl) return null;
  const s = String(nextUrl).trim();
  if (!s) return null;
  if (s.startsWith("http://") || s.startsWith("https://")) return s;
  if (s.startsWith("/")) return `${baseUrl}${s}`;
  return `${baseUrl}/${s}`;
}

// Convert any parseable datetime string to an ISO-like string in Europe/Vilnius.
// Output format: YYYY-MM-DDTHH:mm:ss+02:00 / +03:00 (DST aware)
function toLtIso(value) {
  if (!value) return null;
  const d = new Date(String(value));
  if (Number.isNaN(d.getTime())) return null;
  return formatInTimeZoneIso(d, "Europe/Vilnius");
}

// Returns {year, month, day, hour, minute, second} in a timezone (numbers)
function getTimeZoneParts(date, timeZone) {
  const dtf = new Intl.DateTimeFormat("en-GB", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const parts = dtf.formatToParts(date);
  const map = {};
  for (const p of parts) {
    if (p.type !== "literal") map[p.type] = p.value;
  }
  return {
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
    hour: Number(map.hour),
    minute: Number(map.minute),
    second: Number(map.second),
  };
}

// Build a UTC ISO string that represents "local time in Europe/Vilnius" at Y-M-D h:m:s.
// Used for defaultCreatedAfter/defaultCreatedBefore.
function makeLtLocalIsoUtc(year, month, day, hour, minute, second) {
  // Start with a UTC date at the same wall-clock values, then shift by timezone offset at that moment.
  const approxUtc = new Date(Date.UTC(year, month - 1, day, hour, minute, second));

  // Compute actual offset for Europe/Vilnius at that moment:
  const ltParts = getTimeZoneParts(approxUtc, "Europe/Vilnius");
  const ltAsIfUtc = Date.UTC(
    ltParts.year,
    ltParts.month - 1,
    ltParts.day,
    ltParts.hour,
    ltParts.minute,
    ltParts.second
  );
  const approx = approxUtc.getTime();
  const offsetMs = ltAsIfUtc - approx; // difference between tz wall clock and UTC wall clock
  const trueUtc = new Date(approxUtc.getTime() - offsetMs);
  return trueUtc.toISOString();
}

// Format date as ISO-like string in timezone with numeric offset (+02:00/+03:00).
function formatInTimeZoneIso(date, timeZone) {
  const parts = getTimeZoneParts(date, timeZone);

  // We need offset. Compute offset by comparing tz wall-clock interpreted as UTC vs real UTC time.
  const tzAsIfUtcMs = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second
  );
  const realUtcMs = date.getTime();
  const offsetMin = Math.round((tzAsIfUtcMs - realUtcMs) / 60000); // e.g., +120 / +180

  const sign = offsetMin >= 0 ? "+" : "-";
  const abs = Math.abs(offsetMin);
  const hh = String(Math.floor(abs / 60)).padStart(2, "0");
  const mm = String(abs % 60).padStart(2, "0");

  const Y = String(parts.year).padStart(4, "0");
  const M = String(parts.month).padStart(2, "0");
  const D = String(parts.day).padStart(2, "0");
  const h = String(parts.hour).padStart(2, "0");
  const m = String(parts.minute).padStart(2, "0");
  const s = String(parts.second).padStart(2, "0");

  return `${Y}-${M}-${D}T${h}:${m}:${s}${sign}${hh}:${mm}`;
}

async function fetchJson(url, token) {
  for (let attempt = 1; attempt <= 4; attempt++) {
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
    } finally {
      clearTimeout(timeout);
    }
  }

  throw new Error("Unexpected fetchJson fallthrough");
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
