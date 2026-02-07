import { fetchWithRetry } from "./http.js";

function findEventsArray(json: any): any[] {
  if (Array.isArray(json)) return json;
  if (Array.isArray(json?.data)) return json.data;

  for (const key of ["events", "items", "results"]) {
    if (Array.isArray(json?.[key])) return json[key];
  }

  const data = json?.data;
  if (data && typeof data === "object") {
    for (const key of ["events", "items", "results"]) {
      if (Array.isArray((data as any)[key])) return (data as any)[key];
    }
  }

  return [];
}

function getHasMore(json: any): boolean | null {
  const p = json?.pagination;
  const m = json?.meta;

  for (const obj of [p, m, json]) {
    if (!obj) continue;
    for (const k of ["hasMore", "has_more", "hasNextPage", "has_next_page"]) {
      if (typeof obj[k] === "boolean") return obj[k];
    }
  }
  return null;
}

function getTotal(json: any): number | null {
  const p = json?.pagination;
  for (const obj of [p, json?.meta, json]) {
    if (!obj) continue;
    for (const k of ["total", "totalCount", "total_count"]) {
      if (typeof obj[k] === "number") return obj[k];
    }
  }
  return null;
}

export async function fetchOnePage(params: {
  baseUrl: string;
  apiKey: string;
  cursor: string | null;
  limit: number;
}) {
  const u = new URL(`${params.baseUrl}/events`);
  u.searchParams.set("limit", String(params.limit));
  if (params.cursor) u.searchParams.set("cursor", params.cursor);

  const res = await fetchWithRetry(u.toString(), {
    headers: {
      "X-API-Key": params.apiKey,
      Accept: "application/json",
    },
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${text.slice(0, 300)}`);
  }

  const json = await res.json();
  const events = findEventsArray(json);
  const hasMore = getHasMore(json);
  const total = getTotal(json);

  const nextCursor =
    typeof json?.pagination?.nextCursor === "string" &&
    json.pagination.nextCursor.length > 0
      ? json.pagination.nextCursor
      : null;

  const cursorExpiresIn =
    typeof json?.pagination?.cursorExpiresIn === "number"
      ? json.pagination.cursorExpiresIn
      : null;

  const rateLimit = {
    limit: Number(res.headers.get("x-ratelimit-limit") ?? "0") || null,
    remaining: Number(res.headers.get("x-ratelimit-remaining") ?? "0"),
    resetSeconds: Number(res.headers.get("x-ratelimit-reset") ?? "0") || null,
    retryAfterSeconds: res.headers.get("retry-after")
      ? Number(res.headers.get("retry-after"))
      : null,
  };

  // log discovery on first request (no cursor yet)
  if (!params.cursor) {
    console.log("API discovery:", {
      url: u.toString(),
      topLevelKeys: Object.keys(json ?? {}).slice(0, 25),
      paginationKeys: Object.keys(json?.pagination ?? {}).slice(0, 25),
      eventsLen: events.length,
      hasMore,
      total,
      nextCursorPresent: Boolean(nextCursor),
      cursorExpiresIn,
      rateLimit,
    });
  }

  return { events, hasMore, total, nextCursor, cursorExpiresIn, rateLimit };
}
