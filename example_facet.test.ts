import { Database } from "bun:sqlite";
import { dd, Statin } from ".";
import { test, expect } from "bun:test";

const START_DATE = new Date("2025-03-01 12:00:00").getTime();

// Create or connect to SQLite database
const db = new Database(":memory:", {
  create: true,
  strict: true,
  safeIntegers: true,
});

// Initialize statin tables
dd.init(db);

// Mock account uuid
const accountUuid = crypto.randomUUID();

// Record number of visitors
dd.record({
  db,
  name: "account.num_visitors",
  key: accountUuid,
  val: 1,
  timestamp: START_DATE + 120,
  facets: {
    country: "HK",
    device: "IOS",
  },
});

dd.record({
  db,
  name: "account.num_visitors",
  key: accountUuid,
  val: 1,
  timestamp: START_DATE + 150,
  facets: {
    country: "HK",
    device: "Android",
  },
});

dd.record({
  db,
  name: "account.num_visitors",
  key: accountUuid,
  val: 1,
  timestamp: START_DATE + 190,
  facets: {
    country: "US",
    device: "Android",
  },
});

dd.record({
  db,
  name: "account.num_visitors",
  key: accountUuid,
  val: 1,
  timestamp: START_DATE + 230,
  facets: {
    country: "HK",
    device: "Android",
  },
});

test("facets example", () => {
  // Query for the last recorded value and its statistics
  const stat = dd.get(db, "account.num_visitors", accountUuid, {
    name: "country",
    value: "US",
  });

  // Query for the last two events in descending order
  const events = dd.list(db, "account.num_visitors", accountUuid, {
    limit: 2,
    order: "desc",
  });

  // Query for the last recorded value and its statistics across time intervals
  const result = dd.query(
    db,
    "account.num_visitors",
    accountUuid,
    60 * 1000, // 1 minute interval
    START_DATE, // start time
    START_DATE + 120 * 1000, // end time
    {
      name: "country",
      value: "US",
    },
  );

  expect(stat).toStrictEqual({
    recordedAt: 1740830400190,
    stat: {
      count: 1,
      max: 1,
      min: 1,
      p50: 0.9900000000000001,
      p90: 0.9900000000000001,
      p95: 0.9900000000000001,
      p99: 0.9900000000000001,
      sum: 1,
    },
    value: 1,
  });

  expect(events).toStrictEqual([
    {
      recordedAt: 1740830400230,
      value: 1,
    },
    {
      recordedAt: 1740830400190,
      value: 1,
    },
  ]);

  expect(result).toStrictEqual({
    samples: [
      {
        count: 1,
        end: 1740830460000,
        max: 1,
        min: 1,
        p50: 0.9900000000000001,
        p90: 0.9900000000000001,
        p95: 0.9900000000000001,
        p99: 0.9900000000000001,
        start: 1740830400000,
        sum: 1,
      },
    ],
    stat: {
      count: 1,
      max: 1,
      min: 1,
      p50: 0.9900000000000001,
      p90: 0.9900000000000001,
      p95: 0.9900000000000001,
      p99: 0.9900000000000001,
      sum: 1,
    },
  });
});
