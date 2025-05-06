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
  key: {
    accountUuid,
    country: "US",
  },
  val: 1,
  timestamp: START_DATE + 120,
});

dd.record({
  db,
  name: "account.num_visitors",
  key: {
    accountUuid,
    country: "HK",
  },
  val: 1,
  timestamp: START_DATE + 150,
});

dd.record({
  db,
  name: "account.num_visitors",
  key: {
    accountUuid,
    country: "HK",
  },
  val: 1,
  timestamp: START_DATE + 190,
});

dd.record({
  db,
  name: "account.num_visitors",
  key: {
    accountUuid,
    country: "HK",
  },
  val: 1,
  timestamp: START_DATE + 230,
});

test("composite keys example", () => {
  // Query for the last recorded value and its statistics
  const statUS = dd.get({
    db,
    name: "account.num_visitors",
    key: {
      accountUuid,
      country: "US",
    },
  });

  const statHK = dd.get({
    db,
    name: "account.num_visitors",
    key: {
      accountUuid,
      country: "HK",
    },
  });

  // Query for the last two events in descending order
  const events = dd.list(
    db,
    "account.num_visitors",
    { accountUuid },
    {
      limit: 2,
      order: "desc",
    },
  );

  // Query for the last recorded value and its statistics across time intervals
  const resultHK = dd.query({
    db,
    name: "account.num_visitors",
    key: { accountUuid, country: "HK" },
    duration: 60 * 1000, // 1 minute interval
    start: START_DATE, // start time
    end: START_DATE + 120 * 1000, // end time
  });

  // Query for the last recorded value and its statistics across time intervals
  const resultUS = dd.query({
    db,
    name: "account.num_visitors",
    key: { accountUuid, country: "US" },
    duration: 60 * 1000, // 1 minute interval
    start: START_DATE, // start time
    end: START_DATE + 120 * 1000, // end time
  });

  expect(statUS).toStrictEqual({
    recordedAt: 1740830400120,
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

  expect(statHK).toStrictEqual({
    recordedAt: 1740830400230,
    stat: {
      count: 3,
      max: 1,
      min: 1,
      p50: 0.9900000000000001,
      p90: 0.9900000000000001,
      p95: 0.9900000000000001,
      p99: 0.9900000000000001,
      sum: 3,
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

  expect(resultUS).toStrictEqual({
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

  expect(resultHK).toStrictEqual({
    samples: [
      {
        count: 3,
        end: 1740830460000,
        max: 1,
        min: 1,
        p50: 0.9900000000000001,
        p90: 0.9900000000000001,
        p95: 0.9900000000000001,
        p99: 0.9900000000000001,
        start: 1740830400000,
        sum: 3,
      },
    ],
    stat: {
      count: 3,
      max: 1,
      min: 1,
      p50: 0.9900000000000001,
      p90: 0.9900000000000001,
      p95: 0.9900000000000001,
      p99: 0.9900000000000001,
      sum: 3,
    },
  });
});
