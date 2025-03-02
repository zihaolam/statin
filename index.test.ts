import { test, expect } from "bun:test";
import { dd } from ".";
import { Database } from "bun:sqlite";

test("basic example", () => {
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

  expect(stat).toMatchInlineSnapshot(`
    {
      "recordedAt": 1740830400190,
      "stat": {
        "count": 3n,
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

  // Query for the last recorded value and its statistics across time intervals
  const result = dd.query(
    db,
    "api.response_time",
    "GET /users",
    60 * 1000, // 1 minute interval
    START_DATE, // start time
    START_DATE + 120 * 1000 // end time
  );

  expect(result).toMatchInlineSnapshot(`
    {
      "agg": {
        "count": 3,
        "max": 200,
        "min": 100,
        "p50": 141.1912010207712,
        "p90": 141.1912010207712,
        "p95": 141.1912010207712,
        "p99": 141.1912010207712,
        "sum": 440,
      },
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
    }
  `);
});

test("post like example", () => {
  const db = new Database(":memory:", {
    create: true,
    strict: true,
    safeIntegers: true,
  });

  dd.init(db);

  const like = (postUuid: string, delta: number, now: number = Date.now()) => {
    const result = dd.record(
      db,
      "post.num_likes",
      postUuid,
      (stat) => (stat?.value ?? 0) + delta,
      now
    );

    if (result.status === "updated") {
      const dt = (now - result.recordedAt) / 1000;
      const dv = delta / dt;
      dd.record(db, "post.likes_per_second", postUuid, dv, now);
    }
  };

  const START_DATE = new Date("2025-03-01 12:00:00").getTime();

  like(`cbe563cb-f0fe-476a-9342-d272b9e51325`, 1, START_DATE);
  like(`cbe563cb-f0fe-476a-9342-d272b9e51325`, 1, START_DATE + 1000);
  like(`cbe563cb-f0fe-476a-9342-d272b9e51325`, -1, START_DATE + 2000);
  like(`cbe563cb-f0fe-476a-9342-d272b9e51325`, 1, START_DATE + 3000);

  const numLikes = dd.get(
    db,
    "post.num_likes",
    "cbe563cb-f0fe-476a-9342-d272b9e51325"
  );
  expect(numLikes).toMatchInlineSnapshot(`
    {
      "recordedAt": 1740830403000,
      "stat": {
        "count": 4n,
        "max": 2,
        "min": 1,
        "p50": 0.9900000000000001,
        "p90": 1.9936617014173448,
        "p95": 1.9936617014173448,
        "p99": 1.9936617014173448,
        "sum": 6,
      },
      "value": 2,
    }
  `);

  const likesPerSecond = dd.get(
    db,
    "post.likes_per_second",
    "cbe563cb-f0fe-476a-9342-d272b9e51325"
  );
  expect(likesPerSecond).toMatchInlineSnapshot(`
    {
      "recordedAt": 1740830403000,
      "stat": {
        "count": 3n,
        "max": 1,
        "min": -1,
        "p50": 0.9900000000000001,
        "p90": 0.9900000000000001,
        "p95": 0.9900000000000001,
        "p99": 0.9900000000000001,
        "sum": 1,
      },
      "value": 1,
    }
  `);

  const result = dd.query(
    db,
    "post.likes_per_second",
    "cbe563cb-f0fe-476a-9342-d272b9e51325",
    1000,
    START_DATE,
    START_DATE + 4000
  );

  expect(result).toMatchInlineSnapshot(`
    {
      "agg": {
        "count": 3,
        "max": 1,
        "min": -1,
        "p50": 0.9900000000000001,
        "p90": 0.9900000000000001,
        "p95": 0.9900000000000001,
        "p99": 0.9900000000000001,
        "sum": 1,
      },
      "samples": [
        {
          "count": 1,
          "end": 1740830402000,
          "max": 1,
          "min": 1,
          "p50": 0.9900000000000001,
          "p90": 0.9900000000000001,
          "p95": 0.9900000000000001,
          "p99": 0.9900000000000001,
          "start": 1740830401000,
          "sum": 1,
        },
        {
          "count": 1,
          "end": 1740830403000,
          "max": -1,
          "min": -1,
          "p50": -0.9900000000000001,
          "p90": -0.9900000000000001,
          "p95": -0.9900000000000001,
          "p99": -0.9900000000000001,
          "start": 1740830402000,
          "sum": -1,
        },
        {
          "count": 1,
          "end": 1740830404000,
          "max": 1,
          "min": 1,
          "p50": 0.9900000000000001,
          "p90": 0.9900000000000001,
          "p95": 0.9900000000000001,
          "p99": 0.9900000000000001,
          "start": 1740830403000,
          "sum": 1,
        },
      ],
    }
  `);
});
