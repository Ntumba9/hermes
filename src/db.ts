import { Pool } from "pg";

// Defaults match local dev (docker-compose.yml); overridable for Docker,
// where services address Postgres by container name, not 127.0.0.1.
export const pool = new Pool({
  host: process.env.DB_HOST ?? "127.0.0.1",
  port: Number(process.env.DB_PORT ?? "5434"),
  user: process.env.DB_USER ?? "hermes",
  password: process.env.DB_PASSWORD ?? "hermes",
  database: process.env.DB_NAME ?? "hermes",
  max: Number(process.env.PG_POOL_MAX ?? "10"),
});
