import { createKeyIndices, doesIndexExist } from "./createKeyIndices";
import { generateKeyWhereClause } from "./generateKeyWhereClause";
import { dd } from ".";
import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";

// Create or connect to SQLite database
const db = new Database(":memory:", {
  create: true,
  strict: true,
  safeIntegers: true,
});

dd.init(db);

const key = {
  country: "HK",
  accountUuid: "c17d8659-5401-4b1e-92e3-33c617956062",
  source_href: "reddit.com",
};

dd.record({ db, name: "account.num_visitors", key, val: 1 });

const keyWhere = generateKeyWhereClause(key);

test("indices should be created for composite key fields", () => {
  for (const table of ["stats", "stat_sketches", "events"]) {
    for (const field of keyWhere.fieldsToIndex) {
      expect(doesIndexExist(db, table, field)).toBeTrue();
    }
  }
});
