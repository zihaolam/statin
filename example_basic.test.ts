import { Database } from "bun:sqlite";
import { test, expect } from "bun:test";
import { dd } from ".";

// Create or connect to SQLite database
const db = new Database(":memory:", {
  create: true,
  strict: true,
  safeIntegers: true,
});

// Initialize statin tables
dd.init(db);

// Record API response times
const START_DATE = new Date("2025-03-01 12:00:00").getTime();
dd.record(db, "api.response_time", "GET /users", 100, START_DATE + 120);
dd.record(db, "api.response_time", "GET /users", 200, START_DATE + 150);
dd.record(db, "api.response_time", "GET /users", 140, START_DATE + 190);

// Query for the last recorded value and its statistics
const stat = dd.get(db, "api.response_time", "GET /users");

// Query for the last two events in descending order
const events = dd.list(db, "api.response_time", "GET /users", {
  limit: 2,
  order: "desc",
});

// Query for the last recorded value and its statistics across time intervals
const result = dd.query(
  db,
  "api.response_time",
  "GET /users",
  60 * 1000, // 1 minute interval
  START_DATE, // start time
  START_DATE + 120 * 1000 // end time
);

test("basic example", () => {
  expect(stat).toMatchInlineSnapshot(`
    {
      "recordedAt": 1740830400190,
      "stat": {
        "count": 3,
        "max": 200,
        "min": 100,
        "p50": 141.1912010207712,
        "p90": 141.1912010207712,
        "p95": 141.1912010207712,
        "p99": 141.1912010207712,
        "sum": 440,
      },
      "value": 140,
    }
  `);

  expect(events).toMatchInlineSnapshot(`
    [
      {
        "recordedAt": 1740830400190,
        "value": 140,
      },
      {
        "recordedAt": 1740830400150,
        "value": 200,
      },
    ]
  `);

  expect(result).toMatchInlineSnapshot(`
    {
      "samples": [
        {
          "count": 3,
          "end": 1740830460000,
          "max": 200,
          "min": 100,
          "p50": 141.1912010207712,
          "p90": 141.1912010207712,
          "p95": 141.1912010207712,
          "p99": 141.1912010207712,
          "start": 1740830400000,
          "sum": 440,
        },
      ],
      "stat": {
        "count": 3,
        "max": 200,
        "min": 100,
        "p50": 141.1912010207712,
        "p90": 141.1912010207712,
        "p95": 141.1912010207712,
        "p99": 141.1912010207712,
        "sum": 440,
      },
    }
  `);
});
