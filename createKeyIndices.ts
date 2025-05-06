import { Database } from "bun:sqlite";

export function doesIndexExist(
  db: Database,
  table: string,
  fieldExpression: string,
): boolean {
  const stmt = db.prepare(`
    SELECT 1
    FROM sqlite_master
    WHERE type = 'index'
      AND tbl_name = ?
      AND sql LIKE ?
    LIMIT 1
  `);
  console.info(fieldExpression);
  const dog = db
    .query(`select sql from sqlite_master where type='index' and tbl_name = ?`)
    .all(table);
  console.info({ dog });
  const result = stmt.get(table, `%${fieldExpression}%`);
  return !!result;
}

export function createKeyIndices(db: Database, fieldsToIndex: string[]) {
  for (const table of ["stats", "stat_sketches", "events"]) {
    for (const field of fieldsToIndex) {
      const safeIndexName = field.replace(/[^a-zA-Z0-9_]/g, "_"); // to make it safe for index name, replace special characters
      const sql = `create index if not exists idx_${safeIndexName} on ${table}(${field})`;
      db.run(sql);
    }
  }
}
