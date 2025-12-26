import type { NextApiRequest, NextApiResponse } from "next";

const TZ = "Europe/Vilnius";

// -------- helpers --------
const toNum = (v: any) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

function vilniusParts(date: Date) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: TZ,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    weekday: "short",
    hour12: false,
  })
    .formatToParts(date)
    .reduce((acc: any, p) => ((acc[p.type] = p.value), acc), {});
  return { hh: +parts.hour, mm: +parts.minute, wd: parts.weekday as string };
}

function weekdayNum(wd: string) {
  const map: Record<string, number> = { Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6, Sun: 7 };
  return map[wd] ?? null;
}

function lastSundayOfMonthUTC(year: number, monthIndex0: number) {
  const lastDay = new Date(Date.UTC(year, monthIndex0 + 1, 0));
  const dow = lastDay.getUTCDay(); // 0=Sun
  lastDay.setUTCDate(lastDay.getUTCDate() - dow);
  return lastDay;
}

function isSummerTimeEU(dateUtc: Date) {
  const y = dateUtc.getUTCFullYear();
  const marchLastSunday = lastSundayOfMonthUTC(y, 2);
  const octLastSunday = lastSundayOfMonthUTC(y, 9);

  const dstStart = new Date(Date.UTC(y, 2, marchLastSunday.getUTCDate(), 1, 0, 0)); // 01:00 UTC
  const dstEnd = new Date(Date.UTC(y, 9, octLastSunday.getUTCDate(), 1, 0, 0));     // 01:00 UTC

  return dateUtc >= dstStart && dateUtc < dstEnd;
}

function tarifas(dateUtc: Date) {
  const p = vilniusParts(dateUtc);
  const wd = weekdayNum(p.wd);
  if (wd === 6 || wd === 7) return "Naktinis"; // weekend

  const mins = p.hh * 60 + p.mm;
  const summer = isSummerTimeEU(dateUtc);

  // Summer Mon–Fri: day 08:00–24:00, night 00:00–08:00
  // Winter Mon–Fri: day 07:00–23:00, night 23:00–07:00
  const dayStart = (summer ? 8 : 7) * 60;
  const dayEnd = (summer ? 24 : 23) * 60;

  return mins >= dayStart && mins < dayEnd ? "Dieninis" : "Naktinis";
}

// -------- types --------
type Station = { chargePointId: number; stationName: string };
type Body = { from: string; to: string; stations: Station[] };

async function fetchAllSessionsForStation(
  token: string,
  station: Station,
  from: string,
  to: string
) {
  // include the big flags in list endpoint so we don't need per-session calls
  const base =
    `https://cp.ikrautas.lt/public-api/resources/sessions/v1.0` +
    `?withClockAlignedEnergyConsumption=true` +
    `&clockAlignedInterval=15` +
    `&withAuthorization=true` +
    `&withChargingPeriods=true` +
    `&withChargingPeriodsPriceBreakdown=true` +
    `&withPriceBreakdown=true` +
    `&filter[chargePointId]=${station.chargePointId}` +
    `&filter[startedAfter]=${encodeURIComponent(from)}` +
    `&filter[startedBefore]=${encodeURIComponent(to)}` +
    `&per_page=100&page=1`;

  let url: string | null = base;
  const sessions: any[] = [];

  while (url) {
    const r = await fetch(url, {
      headers: {
        accept: "application/json",
        authorization: `Bearer ${token}`,
      },
    });

    if (!r.ok) {
      const text = await r.text().catch(() => "");
      throw new Error(`Ikrautas API error ${r.status}: ${text.slice(0, 300)}`);
    }

    const j = await r.json();
    const data = Array.isArray(j?.data) ? j.data : [];
    sessions.push(...data);

    url = j?.links?.next || null;
  }

  return sessions;
}

function monthLabelFromFrom(fromIso: string) {
  // use Vilnius month for label
  const d = new Date(fromIso);
  const parts = new Intl.DateTimeFormat("en-CA", { timeZone: TZ, year: "numeric", month: "2-digit" })
    .formatToParts(d)
    .reduce((a: any, p) => ((a[p.type] = p.value), a), {});
  return `${parts.year}-${parts.month}`;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    if (req.method !== "POST") return res.status(405).json({ error: "Use POST" });

    const body = req.body as Body;
    const from = body?.from;
    const to = body?.to;
    const stations = Array.isArray(body?.stations) ? body.stations : [];

    if (!from || !to || stations.length === 0) {
      return res.status(400).json({ error: "Missing {from,to,stations[]}" });
    }

    const token = process.env.IKRAUTAS_BEARER;
    if (!token) return res.status(500).json({ error: "Missing IKRAUTAS_BEARER env var" });

    const windowStart = new Date(from);
    const windowEnd = new Date(to);
    if (Number.isNaN(windowStart.getTime()) || Number.isNaN(windowEnd.getTime())) {
      return res.status(400).json({ error: "Invalid from/to ISO" });
    }

    const monthLabel = monthLabelFromFrom(from);

    const sheets: any[] = [];

    for (const station of stations) {
      const sessions = await fetchAllSessionsForStation(token, station, from, to);

      // aggregate by userId
      const users = new Map<number, {
        userId: number;
        dien_wh: number;
        nak_wh: number;
        intervals: number;
        sessions: Set<string>;
      }>();

      for (const s of sessions) {
        const userId = (s?.userId ?? s?.authorization?.userId ?? 0) as number;
        if (!users.has(userId)) {
          users.set(userId, { userId, dien_wh: 0, nak_wh: 0, intervals: 0, sessions: new Set() });
        }
        const u = users.get(userId)!;
        if (s?.id != null) u.sessions.add(String(s.id));

        const clock = Array.isArray(s?.clockAlignedEnergyConsumption) ? s.clockAlignedEnergyConsumption : [];
        for (const row of clock) {
          const start = new Date(row?.start);
          if (Number.isNaN(start.getTime())) continue;
          if (start < windowStart || start >= windowEnd) continue;

          const wh = toNum(row?.energyConsumption?.total ?? row?.energyConsumed ?? 0);
          if (!(wh > 0)) continue;

          const t = tarifas(start);
          if (t === "Dieninis") u.dien_wh += wh;
          else u.nak_wh += wh;

          u.intervals++;
        }
      }

      const rows = [...users.values()].map(u => {
        const dien_kwh = u.dien_wh / 1000;
        const nak_kwh = u.nak_wh / 1000;
        return {
          month: monthLabel,
          chargePointId: station.chargePointId,
          stationName: station.stationName,
          userId: u.userId,
          dieninis_kwh: Number(dien_kwh.toFixed(3)),
          naktinis_kwh: Number(nak_kwh.toFixed(3)),
          total_kwh: Number((dien_kwh + nak_kwh).toFixed(3)),
          intervalsUsed: u.intervals,
          sessionsUsed: u.sessions.size,
        };
      }).sort((a,b) => (a.userId ?? 0) - (b.userId ?? 0));

      // If nothing useful -> message row
      const hasAnyEnergy = rows.some(r => (r.total_kwh ?? 0) > 0);
      if (!sessions.length || !hasAnyEnergy) {
        sheets.push({
          sheetName: station.stationName,
          rows: [{
            month: monthLabel,
            chargePointId: station.chargePointId,
            stationName: station.stationName,
            message: `Krovimo sesijų per ${monthLabel} mėn. nebuvo`,
          }],
          debug: { sessionsCount: sessions.length }
        });
      } else {
        sheets.push({
          sheetName: station.stationName,
          rows,
          debug: { sessionsCount: sessions.length }
        });
      }
    }

    return res.status(200).json({
      monthLabel,
      from,
      to,
      sheets,
    });
  } catch (e: any) {
    return res.status(500).json({ error: e?.message || "Server error" });
  }
}
