import { Client } from "pg";
async function main() {
    console.log("Starting ingestion service...");
    const db = new Client({
        connectionString: process.env.DATABASE_URL,
    });
    await db.connect();
    console.log("Connected to Postgres");
    await db.end();
}
main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
});
