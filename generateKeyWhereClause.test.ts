// generateKeyWhereClause.test.ts
import { describe, it, expect, beforeEach } from "bun:test";
import { Database, type SQLQueryBindings } from "bun:sqlite";
import { generateKeyWhereClause } from "./generateKeyWhereClause";
import { canonicalize, type JsonType } from "./canonicalize";

type Fixture = { name: string; value: JsonType };

let db: Database;
// 2) define a bunch of test values
const fixtures: Fixture[] = [
  { name: "number", value: 42 },
  { name: "string", value: "hello" },
  { name: "boolean", value: true },
  { name: "null", value: null },
  { name: "array", value: [10, 20] },
  { name: "emptyArr", value: [] },
  { name: "object", value: { a: 1, b: 2 } },
  { name: "emptyObj", value: {} },
  { name: "nestedObj", value: { x: { y: 2 } } },
];

beforeEach(() => {
  // 1) create fresh in-memory DB and table
  db = new Database(":memory:");
  db.run("create table items (key text)");

  // 3) insert each fixture as JSON text
  for (const { value } of fixtures) {
    db.query("INSERT INTO items (key) VALUES (?)").run(canonicalize(value));
  }
});

describe("generateKeyWhereClause()", () => {
  for (const { name, value } of fixtures) {
    it(`matches the ${name} fixture`, () => {
      // Build clause & params
      const { clause, params } = generateKeyWhereClause(value);

      // 1) it should be a non-empty clause
      expect(clause).toBeTruthy();

      // 2) plugging into a SELECT should return exactly one row
      const stmt = db.prepare<{ cnt: number }, SQLQueryBindings[]>(
        `SELECT count(*) AS cnt FROM items WHERE ${clause}`,
      );
      const result = stmt.get(...params);
      expect(result?.cnt).toBe(1);
    });
  }

  it("empty array yields json_array_length(...) = 0 and matches only emptyArr", () => {
    const { clause, params } = generateKeyWhereClause([]);
    expect(clause).toContain("json_array_length");
    // should bind no params for empty array case
    expect(params).toEqual([]);

    // only the one emptyArr row
    const stmt = db.prepare<{ cnt: number }, SQLQueryBindings[]>(
      `SELECT count(*) AS cnt FROM items WHERE ${clause}`,
    );
    const result = stmt.get();
    expect(result?.cnt).toBe(1);

    // verify it’s the correct row
    const row = db
      .prepare<
        { key: string },
        SQLQueryBindings[]
      >(`SELECT key FROM items WHERE ${clause}`)
      .all()[0];
    expect(JSON.parse(row.key)).toEqual([]);
  });

  it("partial-object matching: { a: 1 } should match object and not others", () => {
    const { clause, params } = generateKeyWhereClause({ a: 1 });
    // exactly one matching fixture
    const stmt = db.prepare<{ cnt: number }, SQLQueryBindings[]>(
      `SELECT count(*) AS cnt FROM items WHERE ${clause}`,
    );
    const result = stmt.get(...params);
    expect(result?.cnt).toBe(1);

    // and that one must be our object fixture
    const row = db
      .prepare<
        { key: string },
        SQLQueryBindings[]
      >(`SELECT key FROM items WHERE ${clause}`)
      .all(...params)[0];
    expect(JSON.parse(row.key)).toEqual({ a: 1, b: 2 });
  });

  it("nested-object matching: { x: { y: 2 } } should match nestedObj only", () => {
    const { clause, params } = generateKeyWhereClause({ x: { y: 2 } });
    const stmt = db.prepare<{ cnt: number }, SQLQueryBindings[]>(
      `SELECT count(*) AS cnt FROM items WHERE ${clause}`,
    );
    const result = stmt.get(...params);
    expect(result?.cnt).toBe(1);

    const row = db
      .prepare<
        { key: string },
        SQLQueryBindings[]
      >(`SELECT key FROM items WHERE ${clause}`)
      .all(...params)[0];
    expect(JSON.parse(row.key)).toEqual({ x: { y: 2 } });
  });
});
