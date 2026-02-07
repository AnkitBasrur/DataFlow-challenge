import { Client } from "pg";
import {
  getState,
  saveState,
  insertEvents,
  exportEventIdsToFile,
} from "./db.js";
import { logger } from "./logger.js";
import { fetchOnePage } from "./api.js";

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

  let state = await getState(db);
  state = { ...state, ingested_count: Number(state.ingested_count) as any };
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

  while (true) {
    iter++;

    const resp = await fetchOnePage({
      baseUrl: API_BASE_URL,
      apiKey: API_KEY,
      cursor: state.cursor,
      limit,
    });

    const events = resp.events;
    if (total == null && resp.total) total = resp.total;

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

    let inserted = 0;

    await db.query("BEGIN");
    try {
      inserted = await insertEvents(db, mapped, 500);

      const newCount = Number(state.ingested_count) + inserted;

      await saveState(db, {
        cursor: resp.nextCursor ?? state.cursor,
        ingested_count: newCount as any,
      });

      await db.query("COMMIT");

      state = {
        ...state,
        cursor: resp.nextCursor ?? state.cursor,
        ingested_count: newCount as any,
      };
    } catch (e) {
      await db.query("ROLLBACK");
      throw e;
    }

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
        },
        "Progress",
      );

      lastLogMs = now;
      lastLogCount = Number(state.ingested_count);
    }

    // Stop condition: cursor pagination
    if (resp.hasMore === false || !resp.nextCursor) {
      logger.info(
        { hasMore: resp.hasMore, nextCursor: Boolean(resp.nextCursor) },
        "Reached end",
      );
      break;
    }

    // Rate-limit aware pacing
    const rl = resp.rateLimit;
    if (rl?.retryAfterSeconds && rl.retryAfterSeconds > 0) {
      await sleep(Math.ceil(rl.retryAfterSeconds * 1000) + 150);
    } else if (typeof rl?.remaining === "number" && rl.remaining <= 0) {
      await sleep((rl.resetSeconds ?? 60) * 1000 + 200);
    } else {
      const tiny = Number(process.env.TINY_DELAY_MS ?? "0");
      if (tiny > 0) await sleep(tiny);
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
