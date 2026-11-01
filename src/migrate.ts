import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { pool } from "./db.js";

const migrationsDir = path.join(import.meta.dirname, "..", "migrations");

async function main() {
  const files = (await readdir(migrationsDir)).filter((f) => f.endsWith(".sql")).sort();
  for (const file of files) {
    const sql = await readFile(path.join(migrationsDir, file), "utf8");
    console.log(`Applying ${file}`);
    await pool.query(sql);
  }
  await pool.end();
  console.log("Migrations complete.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
