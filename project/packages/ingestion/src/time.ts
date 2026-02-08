export function extractTsMs(raw: any): number | null {
  const candidates = [
    raw?.timestamp,
    raw?.ts,
    raw?.occurredAt,
    raw?.occurred_at,
    raw?.createdAt,
    raw?.created_at,
    raw?.receivedAt,
    raw?.received_at,
  ];

  for (const v of candidates) {
    if (v == null) continue;

    if (typeof v === "number") return v > 10_000_000_000 ? v : v * 1000;

    if (typeof v === "string") {
      const n = Number(v);
      if (!Number.isNaN(n)) return n > 10_000_000_000 ? n : n * 1000;

      const d = new Date(v);
      if (!Number.isNaN(d.getTime())) return d.getTime();
    }
  }
  return null;
}
