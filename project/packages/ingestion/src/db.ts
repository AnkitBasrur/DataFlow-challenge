import { Client } from "pg";
import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import { to as copyTo } from "pg-copy-streams";

export type IngestionState = {
  cursor: string | null;
  page: number;
  ingested_count: number;
};

export async function getState(db: Client): Promise<IngestionState> {
  const { rows } = await db.query(
    "SELECT cursor, page, ingested_count FROM ingestion_state WHERE id=1",
  );
  return rows[0];
}

export async function saveState(
  db: Client,
  state: Partial<IngestionState>,
): Promise<void> {
  const sets: string[] = [];
  const values: any[] = [];
  let i = 1;

  for (const [k, v] of Object.entries(state)) {
    sets.push(`${k} = $${i++}`);
    values.push(v);
  }
  sets.push(`updated_at = NOW()`);

  await db.query(
    `UPDATE ingestion_state SET ${sets.join(", ")} WHERE id=1`,
    values,
  );
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export async function insertEvents(
  db: Client,
  events: Array<{ id: string; raw: any }>,
  chunkSize = 500,
): Promise<number> {
  if (events.length === 0) return 0;

  let inserted = 0;

  for (const part of chunk(events, chunkSize)) {
    const values: any[] = [];
    const placeholders: string[] = [];
    let p = 1;

    for (const e of part) {
      placeholders.push(`($${p++}, $${p++}::jsonb)`);
      values.push(e.id, JSON.stringify(e.raw));
    }

    const sql = `
      INSERT INTO events (id, raw)
      VALUES ${placeholders.join(", ")}
      ON CONFLICT (id) DO NOTHING
      RETURNING id
    `;

    const res = await db.query(sql, values);
    inserted += res.rowCount ?? 0;
  }

  return inserted;
}

export async function exportEventIdsToFile(db: Client, filepath: string) {
  const out = createWriteStream(filepath, { encoding: "utf8" });

  const copyStream = (db as any).query(
    copyTo(
      "COPY (SELECT id FROM events ORDER BY id) TO STDOUT WITH (FORMAT text)",
    ),
  );

  await pipeline(copyStream, out);
}
