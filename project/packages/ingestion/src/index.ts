import { Client } from "pg";
import {
  getState,
  saveState,
  insertEvents,
  exportEventIdsToFile,
} from "./db.js";
import { logger } from "./logger.js";
import { fetchOnePage } from "./api.js";

function extractTsMs(raw: any): number | null {
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

function getEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env ${name}`);
  return v;
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return "unknown";
  const s = Math.floor(seconds % 60);
  const m = Math.floor((seconds / 60) % 60);
  const h = Math.floor(seconds / 3600);
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

async function main() {
  const exportOnly = process.env.EXPORT_ONLY === "true";

  logger.info(null, "Starting ingestion service...");

  const db = new Client({ connectionString: getEnv("DATABASE_URL") });
  await db.connect();
  logger.info(null, "Connected to Postgres");

  const API_BASE_URL = getEnv("API_BASE_URL");
  const API_KEY = getEnv("API_KEY");

  const limit = Number(process.env.PAGE_SIZE ?? "1000");
  const logEveryIters = Number(process.env.LOG_EVERY_PAGES ?? "5");
  const tinyDelayMs = Number(process.env.TINY_DELAY_MS ?? "0");

  const maxZeroInsertStreak = Number(
    process.env.ZERO_INSERT_RESET_AFTER ?? "8",
  );

  let state = await getState(db);
  state = {
    ...state,
    ingested_count: Number(state.ingested_count) as any,
    last_ts_ms: state.last_ts_ms == null ? null : Number(state.last_ts_ms),
  };
  logger.info(state, "Loaded ingestion_state (resume point)");

  const exportPath = "/app/packages/ingestion/event_ids.txt";

  if (exportOnly) {
    logger.info({ exportPath }, "EXPORT_ONLY=true → exporting event IDs");
    await exportEventIdsToFile(db, exportPath);
    logger.info(null, "Export complete (EXPORT_ONLY)");
    await db.end();
    return;
  }

  let total: number | null = null;

  const startMs = Date.now();
  const startCount = Number(state.ingested_count);
  let lastLogMs = Date.now();
  let lastLogCount = startCount;

  let iter = 0;
  let zeroInsertStreak = 0;

  // Used to detect “cursor not advancing”
  let lastNextCursor: string | null = null;
  let sameNextCursorStreak = 0;

  while (true) {
    iter++;

    const startedCursor = state.cursor; // capture at start for debugging

    // ---- FETCH ----
    let resp: any;
    try {
      resp = await fetchOnePage({
        baseUrl: API_BASE_URL,
        apiKey: API_KEY,
        cursor: state.cursor,
        limit,
      });
    } catch (e: any) {
      const msg = String(e?.message ?? "");

      const isCursorExpired =
        msg.includes("CURSOR_EXPIRED") ||
        msg.includes("Cursor expired") ||
        msg.includes('"code":"CURSOR_EXPIRED"') ||
        msg.includes("expired_cursor_test");

      if (isCursorExpired) {
        logger.warn(
          { cursor: state.cursor },
          "Cursor expired; restarting cursor",
        );
        await saveState(db, { cursor: null });
        state = { ...state, cursor: null };
        continue;
      }

      if (msg.startsWith("BAD_JSON")) {
        logger.warn({ msg }, "Bad JSON from API; retrying shortly");
        await sleep(500);
        continue;
      }

      if (
        msg.includes("API 502") ||
        msg.includes("API 503") ||
        msg.includes("API 504")
      ) {
        logger.warn({ msg }, "Upstream error; retrying shortly");
        await sleep(1000);
        continue;
      }

      throw e;
    }

    const events = resp.events ?? [];
    if (total == null && resp.total) total = resp.total;

    // Cursor debug occasionally
    if (iter % logEveryIters === 0) {
      logger.info(
        {
          iter,
          cursorPresent: Boolean(state.cursor),
          nextCursorPresent: Boolean(resp.nextCursor),
          cursorExpiresIn: resp.cursorExpiresIn ?? null,
          rateRemaining: resp.rateLimit?.remaining ?? null,
        },
        "Cursor debug",
      );
    }

    if (events.length === 0) {
      logger.info({ iter }, "No events returned; stopping");
      break;
    }

    const mapped = events
      .map((e: any) => {
        const id = e?.id ?? e?.eventId ?? e?.event_id;
        if (!id || typeof id !== "string") return null;
        return { id, raw: e };
      })
      .filter(Boolean) as Array<{ id: string; raw: any }>;

    const maxTsSeen = mapped.reduce((acc, e) => {
      const ts = extractTsMs(e.raw);
      return ts != null ? Math.max(acc, ts) : acc;
    }, state.last_ts_ms ?? 0);

    const maxTs =
      state.last_ts_ms != null
        ? Math.max(Number(state.last_ts_ms) + 1, maxTsSeen)
        : maxTsSeen;

    // ---- INSERT + CHECKPOINT (transaction) ----
    let inserted = 0;
    const nextCursor = resp.nextCursor ?? null;

    await db.query("BEGIN");
    try {
      inserted = await insertEvents(db, mapped, 500);
      const newCount = Number(state.ingested_count) + inserted;

      await saveState(db, {
        cursor: nextCursor,
        ingested_count: newCount as any,
        last_ts_ms: maxTs,
      });

      await db.query("COMMIT");

      state = {
        ...state,
        cursor: nextCursor,
        ingested_count: newCount as any,
        last_ts_ms: maxTs,
      };
    } catch (e) {
      await db.query("ROLLBACK");
      throw e;
    }

    // Track whether cursor is advancing
    if (nextCursor && nextCursor === lastNextCursor) sameNextCursorStreak++;
    else {
      sameNextCursorStreak = 0;
      if (nextCursor) lastNextCursor = nextCursor;
    }

    // ---- ZERO INSERT STREAK GUARD ----
    if (inserted === 0) zeroInsertStreak++;
    else zeroInsertStreak = 0;

    if (zeroInsertStreak >= maxZeroInsertStreak) {
      if (sameNextCursorStreak >= 2) {
        logger.warn(
          { zeroInsertStreak, sameNextCursorStreak, startedCursor },
          "0 inserts and cursor not advancing; resetting cursor session",
        );
        await saveState(db, { cursor: null });
        state = { ...state, cursor: null };
      } else {
        logger.warn(
          { zeroInsertStreak, sameNextCursorStreak },
          "0 inserts but cursor is advancing; continuing (likely overlap after reset)",
        );
      }
      zeroInsertStreak = 0;
      continue;
    }

    // ---- PROGRESS LOG ----
    if (iter % logEveryIters === 0) {
      const now = Date.now();
      const elapsedSecTotal = (now - startMs) / 1000;
      const ingestedTotal = Number(state.ingested_count) - startCount;
      const epsTotal = ingestedTotal / Math.max(elapsedSecTotal, 0.001);

      const elapsedSecWindow = (now - lastLogMs) / 1000;
      const ingestedWindow = Number(state.ingested_count) - lastLogCount;
      const epsWindow = ingestedWindow / Math.max(elapsedSecWindow, 0.001);

      let eta = "unknown";
      if (total) {
        const remaining = total - Number(state.ingested_count);
        eta = formatDuration(remaining / Math.max(epsTotal, 0.001));
      }

      logger.info(
        {
          iter,
          insertedLastPage: inserted,
          ingested: Number(state.ingested_count),
          total,
          epsWindow: Math.round(epsWindow),
          epsAvg: Math.round(epsTotal),
          eta,
          cursorPresent: Boolean(state.cursor),
          cursorExpiresIn: resp.cursorExpiresIn ?? null,
        },
        "Progress",
      );

      lastLogMs = now;
      lastLogCount = Number(state.ingested_count);
    }

    // ---- STOP CONDITION ----
    if (resp.hasMore === false) {
      logger.info({ hasMore: resp.hasMore }, "Reached end");
      break;
    }

    if (!resp.nextCursor) {
      // Unexpected; restart cursor session instead of stopping early
      logger.warn(
        "Missing nextCursor but hasMore!=false; restarting cursor session",
      );
      await saveState(db, { cursor: null });
      state = { ...state, cursor: null };
      continue;
    }

    // ---- RATE-LIMIT AWARE PACING ----
    const rl = resp.rateLimit;
    if (rl?.retryAfterSeconds && rl.retryAfterSeconds > 0) {
      await sleep(Math.ceil(rl.retryAfterSeconds * 1000) + 150);
    } else if (typeof rl?.remaining === "number" && rl.remaining <= 0) {
      await sleep((rl.resetSeconds ?? 60) * 1000 + 200);
    } else if (tinyDelayMs > 0) {
      await sleep(tinyDelayMs);
    }
  }

  logger.info(
    { ingested: Number(state.ingested_count), total },
    "Ingestion completed",
  );

  logger.info({ exportPath }, "Exporting event IDs to file...");
  await exportEventIdsToFile(db, exportPath);
  logger.info(null, "Export complete");

  await db.end();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
