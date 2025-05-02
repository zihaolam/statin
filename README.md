# statin

Online time-series analytics and statistics and probabilistic data structures for `bun:sqlite`.

\*\* This project is still a WIP! Scrub through the source code, write some unit tests, help out with documentation, or open up a Github issue if you would like to help out or have any questions!

## Rationale

It is a pain having to deal with an OLAP database in production.

- You need to figure out how to partition what data needs to be recorded in your OLAP database vs. your OLTP database.
- You need to write a lot of code to reliably synchronize necessary data from i.e. your OLTP database to your OLAP database.
- Once you have some order of magnitude of time-series data recorded, practically any kind of query against your data with a OLAP database WILL be slow unless your database supports incrementally maintaining a materialized view of your query results (even if your OLAP database is _columnar_ or _vectorized_ or _embedded_).
- Unless you are using an embedded OLAP database like DuckDB, there is a lot of overhead in sending data over i.e. TCP/IP from your backend to your OLAP database.
- Unless you are using an embedded OLAP database like DuckDB, your tests will require a lot of slow, flaky scaffolding code that i.e. waits until your OLAP database is ready before any actual tests can run.
- You need to learn a new query language, a new API, a slew of new knobs and dials to turn and adjust, and a new ecosystem around your OLAP database of choice.

`statin` deals with incrementally aggregating data in the same OLTP database transaction your backend is already writing to into order statistics (min, max, mean, median, etc.) and quantile sketches.

These quantile sketches incrementally maintain quantiles (p50, p90, p95, p99, etc.) of your data as you record new events with a limited amount of memory.

## Limitations

`statin` requires that newly ingested events have their timestamps monotonically increasing.

To illustrate, the following code will throw an error:

```ts
// GET /users was recorded at 1000ms
dd.record(db, "api.response_time", "GET /users", 120, 1000);
// GET /users was recorded at 500ms (this will throw an error)
dd.record(db, "api.response_time", "GET /users", 120, 500);
// GET /users was recorded at 1500ms (this will not throw an error)
dd.record(db, "api.response_time", "GET /users", 120, 1500);
```

If you need the ability to record events with non-monotonically increasing timestamps, you need to delete and recreate all sketches whose timestamps aregreater than or equal to the timestamp you are recording.

## How it works

`statin` works on top of three tables in your SQLite database:

- `events`: a table that tracks every event recorded.
- `stats`: a table that maintains the latest value of an arbitrary statistic.
- `stat_sketches`: a table that maintains statistical aggregates over multiple configurable time intervals using [DDSketch](https://github.com/DataDog/sketches-js).

DDSketch (from Datadog) is a probabilistic data structure that can be used to estimate the quantiles of a stream of data. It is a compact, approximate representation of the data that can be used to compute quantiles with a guaranteed relative error bound (by default 0.01%).

## Installation

```bash
bun add statin-bun
```

## Usage

### Basic example

```ts
import { Database } from "bun:sqlite";
import { test, expect } from "bun:test";
import { dd } from "statin-bun";

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
  START_DATE + 120 * 1000, // end time
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
```

### Tracking the rate of change of a statistic

```ts
import { Database } from "bun:sqlite";
import { dd } from "statin-bun";
import { test, expect } from "bun:test";

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
    now,
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
  "cbe563cb-f0fe-476a-9342-d272b9e51325",
);

const likesPerSecond = dd.get(
  db,
  "post.likes_per_second",
  "cbe563cb-f0fe-476a-9342-d272b9e51325",
);

const result = dd.query(
  db,
  "post.likes_per_second",
  "cbe563cb-f0fe-476a-9342-d272b9e51325",
  1000,
  START_DATE,
  START_DATE + 4000,
);

test("result", () => {
  expect(numLikes).toMatchInlineSnapshot(`
    {
      "recordedAt": 1740830403000,
      "stat": {
        "count": 4,
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
  expect(likesPerSecond).toMatchInlineSnapshot(`
    {
      "recordedAt": 1740830403000,
      "stat": {
        "count": 3,
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
  expect(result).toMatchInlineSnapshot(`
    {
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
      "stat": {
        "count": 3,
        "max": 1,
        "min": -1,
        "p50": 0.9900000000000001,
        "p90": 0.9900000000000001,
        "p95": 0.9900000000000001,
        "p99": 0.9900000000000001,
        "sum": 1,
      },
    }
  `);
});
```

### Statin with types

```ts
const stats = new Statin<{
  "api.response_time": {
    key: string;
    facets: {
      country: string;
      device: string;
    };
  };
  "post.num_likes": {
    key: {
      likerId: string;
      likedId: string;
    };
    facets: {
      country: string;
      device: string;
    };
  };
  "account.num_visitors": {
    key: {
      visitorId: string;
      creatorId: string;
    };
    facets: {
      country: string;
      device: "IOS" | "Android";
    };
  };
}>();

stats.query({
  name: "account.num_visitors",
  key: { attr1: "123", visitorId: "456" }, // Object literal may only specify known properties, and 'attr1' does not exist in type '{ visitorId: string; creatorId: string; }'.
  facet: { name: "device", value: 5 }, // Type 'number' is not assignable to type '"IOS" | "Android"'.
  db,
  start: START_DATE,
  end: START_DATE + 120 * 1000,
  duration: 20000,
});

stats.query({
  db: new Database(":memory:"),
  name: "acc.num_visitors", // Type '"acc.num_visitors"' is not assignable to type '"account.num_visitors" | "api.response_time" | "post.num_likes"'.
  key: {
    visitorId: "123",
    creatorId: "456",
  },
  duration: 1000,
  start: Date.now(),
  end: Date.now() + 1000,
  facet: {
    name: "source_href", // Type '"source_href"' is not assignable to type '"country" | "device"'.
    value: "reddit.com",
  },
});
```

### Recommended TypeScript Configuration

If you’re consuming `statin` from a TypeScript project, we strongly recommend enabling these `compilerOptions` in your `tsconfig.json` to get full type‐safety and precise autocomplete on facets:

```jsonc
{
  "compilerOptions": {
    // turn on all strict type-checking options
    "strict": true,

    // treat foo?: T *exactly* as T (not T | undefined)
    // this prevents passing `undefined` as a facet value
    "exactOptionalPropertyTypes": true,
  }
}


## Roadmap

Additional probabilistic data structures will be added in the future.

- Count-Min Sketch for tracking the frequency of items in a stream
- Top-K solutions for tracking the top K items in a stream
- HyperLogLog for cardinality estimation

## License

[MIT](LICENSE)
```
