import { Database } from "bun:sqlite";
import { dd } from ".";
import { test, expect } from "bun:test";

const db = new Database(":memory:", {
  create: true,
  strict: true,
  safeIntegers: true,
});

dd.init(db);

const like = (postUuid: string, delta: number, now: number = Date.now()) => {
  const result = dd.record({
    db,
    name: "post.num_likes",
    key: postUuid,
    val: (stat) => (stat?.value ?? 0) + delta,
    timestamp: now,
  });

  if (result.status === "updated") {
    const dt = (now - result.recordedAt) / 1000;
    const dv = delta / dt;
    dd.record({
      db,
      name: "post.likes_per_second",
      key: postUuid,
      val: dv,
      timestamp: now,
    });
  }
};

const START_DATE = new Date("2025-03-01 12:00:00").getTime();

like(`cbe563cb-f0fe-476a-9342-d272b9e51325`, 1, START_DATE);
like(`cbe563cb-f0fe-476a-9342-d272b9e51325`, 1, START_DATE + 1000);
like(`cbe563cb-f0fe-476a-9342-d272b9e51325`, -1, START_DATE + 2000);
like(`cbe563cb-f0fe-476a-9342-d272b9e51325`, 1, START_DATE + 3000);

const numLikes = dd.get({
  db,
  name: "post.num_likes",
  key: "cbe563cb-f0fe-476a-9342-d272b9e51325",
});

const likesPerSecond = dd.get({
  db,
  name: "post.likes_per_second",
  key: "cbe563cb-f0fe-476a-9342-d272b9e51325",
});

const result = dd.query({
  db,
  name: "post.likes_per_second",
  key: "cbe563cb-f0fe-476a-9342-d272b9e51325",
  duration: 1000,
  start: START_DATE,
  end: START_DATE + 4000,
});

test("rate of change example", () => {
  expect(numLikes).toStrictEqual({
    recordedAt: 1740830403000,
    stat: {
      count: 4,
      max: 2,
      min: 1,
      p50: 0.9900000000000001,
      p90: 1.9936617014173448,
      p95: 1.9936617014173448,
      p99: 1.9936617014173448,
      sum: 6,
    },
    value: 2,
  });
  expect(likesPerSecond).toStrictEqual({
    recordedAt: 1740830403000,
    stat: {
      count: 3,
      max: 1,
      min: -1,
      p50: 0.9900000000000001,
      p90: 0.9900000000000001,
      p95: 0.9900000000000001,
      p99: 0.9900000000000001,
      sum: 1,
    },
    value: 1,
  });
  expect(result).toStrictEqual({
    samples: [
      {
        count: 1,
        end: 1740830402000,
        max: 1,
        min: 1,
        p50: 0.9900000000000001,
        p90: 0.9900000000000001,
        p95: 0.9900000000000001,
        p99: 0.9900000000000001,
        start: 1740830401000,
        sum: 1,
      },
      {
        count: 1,
        end: 1740830403000,
        max: -1,
        min: -1,
        p50: -0.9900000000000001,
        p90: -0.9900000000000001,
        p95: -0.9900000000000001,
        p99: -0.9900000000000001,
        start: 1740830402000,
        sum: -1,
      },
      {
        count: 1,
        end: 1740830404000,
        max: 1,
        min: 1,
        p50: 0.9900000000000001,
        p90: 0.9900000000000001,
        p95: 0.9900000000000001,
        p99: 0.9900000000000001,
        start: 1740830403000,
        sum: 1,
      },
    ],
    stat: {
      count: 3,
      max: 1,
      min: -1,
      p50: 0.9900000000000001,
      p90: 0.9900000000000001,
      p95: 0.9900000000000001,
      p99: 0.9900000000000001,
      sum: 1,
    },
  });
});
