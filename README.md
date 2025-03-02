# statin

Time-series analytics and statistics and probabilistic data structures for `bun:sqlite`.

\*\* This project is still a WIP! Scrub through the source code, write some unit tests, help out with documentation, or open up a Github issue if you would like to help out or have any questions!

## Rationale

It is a pain having to deal with an OLAP database in production.

- You need to figure out how to partition what data needs to be recorded in your OLAP database vs. your OLTP database.
- You need to write a lot of code to reliably synchronize necessary data from i.e. your OLTP database to your OLAP database.
- Once you have some order of magnitude of time-series data recorded, practically any kind of query against your data with a OLAP database WILL be slow unless your database supports incrementally maintaining a materialized view of your query results (even if your OLAP database is _columnar_ or _vectorized_ or _embedded_).
- Unless you are using an embedded OLAP database like DuckDB, there is a lot of overhead in sending data over i.e. TCP/IP from your backend to your OLAP database.
- Unless you are using an embedded OLAP database like DuckDB, your tests will require a lot of slow, flaky scaffolding code that i.e. waits until your OLAP database is ready before any actual tests can run.
- You need to learn a new query language, a new API, a slew of new knobs and dials to turn and adjust, and a new ecosystem around your OLAP database of choice.

`statin` deals with aggregating data in the same OLTP database transaction your backend is already writing to into statistical sketches.

These statistical sketches incrementally maintain order statistics (min, max, mean, median, etc.) and quantiles (p50, p90, p95, p99, etc.) of your data.

## Limitations

`statin` requires that newly recorded statistics have their timestamps monotonically increasing.

To illustrate, the following code will throw an error:

```ts
// GET /users was recorded at 1000ms
dd.record(db, "api.response_time", "GET /users", 120, 1000);
// GET /users was recorded at 500ms (this will throw an error)
dd.record(db, "api.response_time", "GET /users", 120, 500);
// GET /users was recorded at 1500ms (this will not throw an error)
dd.record(db, "api.response_time", "GET /users", 120, 1500);
```

If you need the ability to record statistics at arbitrary timestamps, you need to delete and recreate all sketches greater than or equal to the timestamp you are recording.

## How it works

`statin` works on top of two tables in your SQLite database:

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

// {
//     "recordedAt": 1740830400190,
//     "stat": {
//         "count": 3n,
//         "max": 200,
//         "min": 100,
//         "p50": 141.1912010207712,
//         "p90": 141.1912010207712,
//         "p95": 141.1912010207712,
//         "p99": 141.1912010207712,
//         "sum": 440,
//     },
//     "value": 140,
// }

// Query for the last recorded value and its statistics across time intervals
const result = dd.query(
  db,
  "api.response_time",
  "GET /users",
  60 * 1000, // 1 minute interval
  START_DATE, // start time
  START_DATE + 120 * 1000 // end time
);

// {
//     "agg": {
//         "count": 3,
//         "max": 200,
//         "min": 100,
//         "p50": 141.1912010207712,
//         "p90": 141.1912010207712,
//         "p95": 141.1912010207712,
//         "p99": 141.1912010207712,
//         "sum": 440,
//     },
//     "samples": [
//         {
//             "count": 3,
//             "end": 1740830460000,
//             "max": 200,
//             "min": 100,
//             "p50": 141.1912010207712,
//             "p90": 141.1912010207712,
//             "p95": 141.1912010207712,
//             "p99": 141.1912010207712,
//             "start": 1740830400000,
//             "sum": 440,
//         },
//     ],
// }
```

### Tracking the rate of change of a statistic

```ts
import { Database } from "bun:sqlite";
import { dd } from "statin-bun";

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

const result = dd.query(
  db,
  "post.likes_per_second",
  "cbe563cb-f0fe-476a-9342-d272b9e51325",
  1000,
  START_DATE,
  START_DATE + 4000
);

// {
//     "agg": {
//         "count": 3,
//         "max": 1,
//         "min": -1,
//         "p50": 0.9900000000000001,
//         "p90": 0.9900000000000001,
//         "p95": 0.9900000000000001,
//         "p99": 0.9900000000000001,
//         "sum": 1,
//     },
//     "samples": [
//         {
//             "count": 1,
//             "end": 1740830402000,
//             "max": 1,
//             "min": 1,
//             "p50": 0.9900000000000001,
//             "p90": 0.9900000000000001,
//             "p95": 0.9900000000000001,
//             "p99": 0.9900000000000001,
//             "start": 1740830401000,
//             "sum": 1,
//         },
//         {
//             "count": 1,
//             "end": 1740830403000,
//             "max": -1,
//             "min": -1,
//             "p50": -0.9900000000000001,
//             "p90": -0.9900000000000001,
//             "p95": -0.9900000000000001,
//             "p99": -0.9900000000000001,
//             "start": 1740830402000,
//             "sum": -1,
//         },
//         {
//             "count": 1,
//             "end": 1740830404000,
//             "max": 1,
//             "min": 1,
//             "p50": 0.9900000000000001,
//             "p90": 0.9900000000000001,
//             "p95": 0.9900000000000001,
//             "p99": 0.9900000000000001,
//             "start": 1740830403000,
//             "sum": 1,
//         },
//     ],
// }
```

## Roadmap

Additional probabilistic data structures will be added in the future.

- Count-Min Sketch for tracking the frequency of items in a stream
- Top-K solutions for tracking the top K items in a stream
- HyperLogLog for cardinality estimation

## License

[MIT](LICENSE)
