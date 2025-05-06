import { Database } from "bun:sqlite";

export function createKeyIndices(db: Database, fieldsToIndex: string[]) {
  for (const table of ["stats", "stat_sketches", "events"]) {
    for (const field of fieldsToIndex) {
      const safeIndexName = field.replace(/[^a-zA-Z0-9_]/g, "_"); // to make it safe for index name, replace special characters
      const sql = `create index if not exists idx_${table}_${safeIndexName} on ${table}(${field})`;
      db.run(sql);
    }
  }
}
