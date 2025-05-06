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
dd.record({
  db,
  name: "api.response_time",
  key: "GET /users",
  val: 100,
  timestamp: START_DATE + 120,
});
dd.record({
  db,
  name: "api.response_time",
  key: "GET /users",
  val: 200,
  timestamp: START_DATE + 150,
});
dd.record({
  db,
  name: "api.response_time",
  key: "GET /users",
  val: 140,
  timestamp: START_DATE + 190,
});

// Example for recording number of views
for (let i = 0; i < 5; i++) {
  dd.record({
    db,
    name: "post.num_views",
    key: {
      postId: "1",
      viewerId: "bob",
    },
    val: 1,
    timestamp: START_DATE + i,
  });
}

for (let i = 0; i < 10; i++)
  dd.record({
    db,
    name: "post.num_views",
    key: {
      postId: "1",
      viewerId: "alice",
    },
    val: 1,
    timestamp: START_DATE + i,
  });

for (let i = 0; i < 10; i++)
  dd.record({
    db,
    name: "post.num_views",
    key: {
      postId: "1",
      viewerId: "alice",
    },
    val: 1,
    timestamp: START_DATE + 80 * 1000 + i,
  });

// Query for the last recorded value and its statistics
const stat = dd.get({ db, name: "api.response_time", key: "GET /users" });

// Query for the last two events in descending order
const events = dd.list(db, "api.response_time", "GET /users", {
  limit: 2,
  order: "desc",
});

// Query for the last recorded value and its statistics across time intervals
const result = dd.query({
  db,
  name: "api.response_time",
  key: "GET /users",
  duration: 60 * 1000, // 1 minute interval
  start: START_DATE, // start time
  end: START_DATE + 120 * 1000, // end time
});

const topPosts = dd.find({
  db,
  name: "post.num_views",
  key: {
    postId: "1",
  },
  start: START_DATE,
  end: START_DATE + 120 * 1000,
  duration: 60 * 1000,
  opts: {
    order: "sum desc",
    limit: 10,
  },
});

test("basic example", () => {
  expect(stat).toStrictEqual({
    recordedAt: 1740830400190,
    stat: {
      count: 3,
      max: 200,
      min: 100,
      p50: 141.1912010207712,
      p90: 141.1912010207712,
      p95: 141.1912010207712,
      p99: 141.1912010207712,
      sum: 440,
    },
    value: 140,
  });

  expect(events).toStrictEqual([
    {
      recordedAt: 1740830400190,
      value: 140,
    },
    {
      recordedAt: 1740830400150,
      value: 200,
    },
  ]);

  expect(result).toStrictEqual({
    samples: [
      {
        count: 3,
        end: 1740830460000,
        max: 200,
        min: 100,
        p50: 141.1912010207712,
        p90: 141.1912010207712,
        p95: 141.1912010207712,
        p99: 141.1912010207712,
        start: 1740830400000,
        sum: 440,
      },
    ],
    stat: {
      count: 3,
      max: 200,
      min: 100,
      p50: 141.1912010207712,
      p90: 141.1912010207712,
      p95: 141.1912010207712,
      p99: 141.1912010207712,
      sum: 440,
    },
  });

  expect(topPosts).toStrictEqual([
    {
      key: { postId: "1", viewerId: "alice" },
      count: 20,
      sum: 20,
      min: 1,
      max: 1,
    },
    {
      key: { postId: "1", viewerId: "bob" },
      count: 5,
      sum: 5,
      min: 1,
      max: 1,
    },
  ]);
});
