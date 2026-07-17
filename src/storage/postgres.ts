import { Pool } from 'pg';
import fs from 'fs';
import path from 'path';

export function createPool(): Pool {
  return new Pool({
    host: process.env.PGHOST ?? 'localhost',
    port: parseInt(process.env.PGPORT ?? '5432', 10),
    user: process.env.PGUSER ?? 'ratelimiter',
    password: process.env.PGPASSWORD ?? 'ratelimiter',
    database: process.env.PGDATABASE ?? 'ratelimiter',
    max: 10,
  });
}

export async function runMigrations(pool: Pool): Promise<void> {
  const dir = path.join(__dirname, '..', '..', 'storage', 'migrations');
  const files = fs.readdirSync(dir).filter((f) => f.endsWith('.sql')).sort();
  for (const file of files) {
    const sql = fs.readFileSync(path.join(dir, file), 'utf-8');
    await pool.query(sql);
  }
}
