import { DDSketch } from "./ddsketch";
import { Database, type SQLQueryBindings } from "bun:sqlite";
import { outdent } from "outdent";
import { canonicalize, type JsonRecord, type JsonType } from "./canonicalize";
import { generateKeyWhereClause } from "./generateKeyWhereClause";
import { createKeyIndices } from "./createKeyIndices";

export { DDSketch };
export { KeyMapping, LogarithmicMapping, DenseStore } from "./ddsketch";

type Key = JsonType;
type PartialKey<TKey extends Key> = TKey extends infer T
  ? T extends JsonRecord
    ? Partial<T>
    : T
  : never;

export namespace dd {
  export const DEFAULT_INTERVAL_DURATIONS: number[] = [
    1000, // second
    60 * 1000, // minute
    60 * 60 * 1000, // hour
    24 * 60 * 60 * 1000, // day
    7 * 24 * 60 * 60 * 1000, // week
    30 * 24 * 60 * 60 * 1000, // month
    365 * 24 * 60 * 60 * 1000, // year
  ];

  function serializeKey(key: Key) {
    return canonicalize(key);
  }

  export function init(db: Database) {
    db.exec(outdent`
        create table if not exists events (
            name text not null,
            key text not null,
            recorded_at datetime not null,
            val real not null,
            primary key (name, key, recorded_at)
        ) without rowid;

        create table if not exists stats (
            name text not null,
            key text not null,
            val real not null,
            recorded_at datetime not null,
            sum real not null default 0,
            count real not null default 0,
            min real not null default 0,
            max real not null default 0,
            p50 real not null default 0,
            p90 real not null default 0,
            p95 real not null default 0,
            p99 real not null default 0,
            sketch blob,
            primary key (name, key)
        ) without rowid;

        create table if not exists stat_sketches (
            name text not null,
            key text not null,
            duration integer not null,
            start datetime not null,
            end datetime not null,
            count real not null default 0,
            sum real not null default 0,
            min real not null default 0,
            max real not null default 0,
            p50 real not null default 0,
            p90 real not null default 0,
            p95 real not null default 0,
            p99 real not null default 0,
            sketch blob,
            primary key (name, key, duration, start)
        ) without rowid;
    `);
  }

  export function record({
    db,
    name,
    key,
    val,
    timestamp = Date.now(),
    intervals = DEFAULT_INTERVAL_DURATIONS,
  }: {
    db: Database;
    name: string;
    key: Key;
    val: number | ((stat?: { value: number; recordedAt: number }) => number);
    timestamp?: number;
    intervals?: number[];
  }) {
    const keyWhere = generateKeyWhereClause(key);

    createKeyIndices(db, keyWhere.fieldsToIndex);

    const stat = aggregateGet({
      db,
      name,
      key,
    });

    const now = timestamp;
    const serializedKey = serializeKey(key);

    if (stat === null) {
      let next = val;
      if (typeof next === "function") {
        next = next();
      }

      db.query(
        `insert into events (name, key, val, recorded_at) values (?, ?, ?, ?)`,
      ).run(name, serializedKey, next, timestamp);

      const sketch = new DDSketch();

      sketch.add(next);
      const count = 1;
      const sum = next;
      const min = next;
      const max = next;

      db.query(
        `insert into stats (name, key, val, recorded_at, sketch, min, max, count, sum, p50, p90, p95, p99) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      ).run(
        name,
        serializedKey,
        next,
        now,
        sketch.serialize(),
        min,
        max,
        count,
        sum,
        sketch.getValueAtQuantile(0.5, { count }),
        sketch.getValueAtQuantile(0.9, { count }),
        sketch.getValueAtQuantile(0.95, { count }),
        sketch.getValueAtQuantile(0.99, { count }),
      );

      for (const interval of intervals) {
        dd.sketch(db, name, key, next, now, interval);
      }

      return {
        status: "created" as const,
        value: next,
        recordedAt: now,
      };
    }

    let next = val;
    if (typeof next === "function") {
      next = next({
        value: Number(stat.value),
        recordedAt: Number(stat.recordedAt),
      });
    }

    db.query(
      `insert into events (name, key, val, recorded_at) values (?, ?, ?, ?)`,
    ).run(name, serializedKey, next, timestamp);

    stat.recordedAt = Number(stat.recordedAt);

    // If the timestamp is in the future, we can't record it.
    if (stat.recordedAt >= now) {
      throw new RangeError("Timestamp is in the past.");
    }

    const sketch = stat.sketch;

    sketch.add(next);

    const count = stat.count + 1;
    const sum = stat.sum + next;
    const min = Math.min(stat.min, next);
    const max = Math.max(stat.max, next);

    db.query(
      `update stats set val = ?, recorded_at = ?, sketch = ?, min = ?, max = ?, count = ?, sum = ?, p50 = ?, p90 = ?, p95 = ?, p99 = ? where name = ? and ${keyWhere.clause};`,
    ).run(
      next,
      now,
      sketch.serialize(),
      min,
      max,
      count,
      sum,
      sketch.getValueAtQuantile(0.5, { count }),
      sketch.getValueAtQuantile(0.9, { count }),
      sketch.getValueAtQuantile(0.95, { count }),
      sketch.getValueAtQuantile(0.99, { count }),
      name,
      ...keyWhere.params,
    );

    for (const interval of intervals) {
      dd.sketch(db, name, key, next, now, interval);
    }

    return {
      status: "updated" as const,
      value: next,
      recordedAt: stat.recordedAt,
    };
  }

  export function sketch(
    db: Database,
    name: string,
    key: Key,
    val: number,
    timestamp: number,
    interval: number,
  ) {
    const start = Math.floor(timestamp / interval) * interval;
    const end = start + interval;
    const keyWhere = generateKeyWhereClause(key);

    const cached = db
      .query<
        {
          sketch: Uint8Array;
          count: number;
          min: number;
          max: number;
          sum: number;
        },
        SQLQueryBindings[]
      >(
        outdent`
            select sketch, min, max, count, sum from stat_sketches where name = ? and duration = ? and start = ? and ${keyWhere.clause};
        `,
      )
      .get(name, interval, start, ...keyWhere.params);

    let sketch: DDSketch;
    let count = 0;
    let sum = 0;
    let min = Infinity;
    let max = -Infinity;

    if (cached !== null) {
      sketch = DDSketch.deserialize(cached.sketch);
      count = cached.count;
      sum = cached.sum;
      min = cached.min;
      max = cached.max;
    } else {
      sketch = new DDSketch();
    }

    sketch.add(val);
    count += 1;
    sum += val;
    min = Math.min(min, val);
    max = Math.max(max, val);

    db.query(
      outdent`
        insert into stat_sketches (name, key, duration, start, end, count, sum, min, max, p50, p90, p95, p99, sketch)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict (name, key, duration, start) do update set
        count = excluded.count,
        sum = excluded.sum,
        min = excluded.min,
        max = excluded.max,
        p50 = excluded.p50,
        p90 = excluded.p90,
        p95 = excluded.p95,
        p99 = excluded.p99,
        sketch = excluded.sketch;
      `,
    ).run(
      name,
      serializeKey(key),
      interval,
      start,
      end,
      count,
      sum,
      min,
      max,
      sketch.getValueAtQuantile(0.5, { count }),
      sketch.getValueAtQuantile(0.9, { count }),
      sketch.getValueAtQuantile(0.95, { count }),
      sketch.getValueAtQuantile(0.99, { count }),
      sketch.serialize(),
    );
  }

  export function list(
    db: Database,
    name: string,
    key: Key,
    opts?: {
      range?: { start: number; end: number };
      limit?: number;
      order?: "asc" | "desc";
    },
  ) {
    const keyWhere = generateKeyWhereClause(key);
    let clause = `where name = ? and ${keyWhere.clause}`;
    let params: SQLQueryBindings[] = [name, ...keyWhere.params];

    if (opts?.range !== undefined) {
      clause += " and recorded_at >= ? and recorded_at <= ?";
      params.push(opts.range.start, opts.range.end);
    }

    clause += ` order by recorded_at ${opts?.order ?? "desc"}`;
    clause += ` limit ${opts?.limit ?? 100}`;

    const events = db
      .query<
        {
          val: number;
          recorded_at: number | bigint;
        },
        SQLQueryBindings[]
      >(`select val, recorded_at from events ${clause}`)
      .all(...params);

    return events.map((event) => ({
      value: event.val,
      recordedAt: Number(event.recorded_at),
    }));
  }

  function aggregateGet({
    db,
    name,
    key,
  }: {
    db: Database;
    name: string;
    key: Key;
  }) {
    const keyWhere = generateKeyWhereClause(key);
    const stats = db
      .query<
        {
          val: number;
          recorded_at: number | bigint;
          min: number;
          max: number;
          count: number;
          sum: number;
          p50: number;
          p90: number;
          p95: number;
          p99: number;
          sketch: Uint8Array;
        },
        SQLQueryBindings[]
      >(
        outdent`
          select val, recorded_at, min, max, count, sum, p50, p90, p95, p99, sketch from stats
          where name = ? and ${keyWhere.clause} order by recorded_at asc;
        `,
      )
      .all(name, ...keyWhere.params);

    if (stats.length === 0) {
      return null;
    }

    let sketch!: DDSketch;
    let recordedAt!: number | bigint;
    let value!: number;

    let count = 0;
    let sum = 0;
    let min = Infinity;
    let max = -Infinity;

    for (const stat of stats) {
      const decoded = DDSketch.deserialize(stat.sketch);
      if (sketch === undefined) {
        sketch = decoded;
      } else {
        sketch.merge(decoded);
      }
      count += stat.count;
      sum += stat.sum;
      min = Math.min(min, stat.min);
      max = Math.max(max, stat.max);
      recordedAt = stat.recorded_at;
      value = stat.val;
    }

    return {
      count,
      sum,
      min,
      max,
      sketch,
      recordedAt: Number(recordedAt),
      value,
    };
  }

  export function get({
    db,
    name,
    key,
  }: {
    db: Database;
    name: string;
    key: Key;
  }) {
    const stat = aggregateGet({ db, name, key });

    if (stat === null) {
      return null;
    }

    const { min, max, count, sum, sketch, value } = stat;

    return {
      value,
      recordedAt: Number(stat.recordedAt),
      stat: {
        min,
        max,
        count,
        sum,
        p50: sketch.getValueAtQuantile(0.5, { count }),
        p90: sketch.getValueAtQuantile(0.9, { count }),
        p95: sketch.getValueAtQuantile(0.95, { count }),
        p99: sketch.getValueAtQuantile(0.99, { count }),
      },
    };
  }

  export function query({
    db,
    name,
    key,
    duration,
    start,
    end,
  }: {
    db: Database;
    name: string;
    key: Key;
    duration: number;
    start: number;
    end: number;
  }) {
    const keyWhere = generateKeyWhereClause(key);

    const rows = db
      .query<
        {
          start: number | bigint;
          end: number | bigint;
          key: string;
          count: number;
          sum: number;
          min: number;
          max: number;
          p50: number;
          p90: number;
          p95: number;
          p99: number;
          sketch: Uint8Array;
        },
        SQLQueryBindings[]
      >(
        outdent`
          select start, end, key, count, sum, min, max, p50, p90, p95, p99, sketch
          from stat_sketches 
          where name = ? and duration = ? and start >= ? and end <= ? and ${keyWhere.clause}
          order by start asc;
        `,
      )
      .all(name, duration, start, end, ...keyWhere.params);

    // two different streams of stat_sketches, we want to aggregate by start and end, then merge the sketches grouped by the start and end
    const groupedSamples = new Map<
      string,
      {
        start: number;
        end: number;
        count: number;
        sum: number;
        min: number;
        max: number;
        p50: number;
        p90: number;
        p95: number;
        p99: number;
      }
    >();

    let stat: DDSketch | null = null;
    let count = 0;
    let sum = 0;
    let min = Infinity;
    let max = -Infinity;

    for (const { sketch, ...row } of rows) {
      const decoded = DDSketch.deserialize(sketch);
      if (stat === null) {
        stat = decoded;
      } else {
        stat.merge(decoded);
      }

      count += row.count;
      sum += row.sum;
      min = Math.min(min, row.min);
      max = Math.max(max, row.max);

      const existing = groupedSamples.get(`${row.start}-${row.end}`);
      if (existing === undefined) {
        groupedSamples.set(`${row.start}-${row.end}`, {
          start: Number(row.start),
          end: Number(row.end),
          count: row.count,
          sum: row.sum,
          min: row.min,
          max: row.max,
          p50: row.p50,
          p90: row.p90,
          p95: row.p95,
          p99: row.p99,
        });
      } else {
        existing.count += row.count;
        existing.sum += row.sum;
        existing.min = Math.min(existing.min, row.min);
        existing.max = Math.max(existing.max, row.max);
        existing.p50 = stat.getValueAtQuantile(0.5, { count });
        existing.p90 = stat.getValueAtQuantile(0.9, { count });
        existing.p95 = stat.getValueAtQuantile(0.95, { count });
        existing.p99 = stat.getValueAtQuantile(0.99, { count });
      }
    }

    if (stat === null) {
      return null;
    }

    return {
      stat: {
        count,
        sum,
        min,
        max,
        p50: stat.getValueAtQuantile(0.5, { count }),
        p90: stat.getValueAtQuantile(0.9, { count }),
        p95: stat.getValueAtQuantile(0.95, { count }),
        p99: stat.getValueAtQuantile(0.99, { count }),
      },
      samples: Array.from(groupedSamples.values()),
    };
  }

  export function find<TKey extends Key = Key>({
    db,
    key,
    name,
    opts,
    start,
    end,
    duration,
  }: {
    db: Database;
    name: string;
    key: Key;
    start: number;
    end: number;
    duration: number;
    opts?: {
      limit?: number;
      order?: `${"sum" | "count" | "min" | "max"} ${"asc" | "desc"}`;
    };
  }) {
    const keyWhere = generateKeyWhereClause(key);
    let clause = `where name = ? and ${keyWhere.clause}`;
    let params: SQLQueryBindings[] = [name, ...keyWhere.params];

    clause += " and start >= ? and end <= ? and duration = ?";
    params.push(start, end, duration);

    clause += " group by key";
    clause += " order by ?";
    clause += " limit ?";
    params.push(opts?.order ?? "sum desc", opts?.limit ?? 20);

    const stats = db
      .query<
        {
          key: string;
          count: number;
          sum: number;
          min: number;
          max: number;
        },
        SQLQueryBindings[]
      >(
        outdent`
            select key, sum(count) as count, sum(sum) as sum, min(min) as min, max(max) as max from stat_sketches ${clause};
        `,
      )
      .all(...params);

    return stats.map((stat) => ({
      ...stat,
      key: JSON.parse(stat.key) as Key,
    }));
  }
}

export type StatinSchema = Record<string, Key>;

export const statin = <TKey extends Key>({ name }: { name: string }) => {
  return {
    record({
      db,
      key,
      val,
      timestamp,
      intervals,
    }: {
      db: Database;
      key: PartialKey<TKey>;
      val: number | ((stat?: { value: number; recordedAt: number }) => number);
      timestamp?: number;
      intervals?: number[];
    }) {
      return dd.record({
        db,
        name,
        key: key as Key,
        val,
        timestamp,
        intervals,
      });
    },
    sketch({
      db,
      key,
      val,
      timestamp,
      interval,
    }: {
      db: Database;
      key: PartialKey<TKey>;
      val: number;
      timestamp: number;
      interval: number;
    }) {
      return dd.sketch(db, name, key as Key, val, timestamp, interval);
    },
    get({ db, key }: { db: Database; key: PartialKey<TKey> }) {
      return dd.get({
        db,
        name,
        key: key as Key,
      });
    },
    query({
      db,
      key,
      duration,
      start,
      end,
    }: {
      db: Database;
      key: PartialKey<TKey>;
      duration: number;
      start: number;
      end: number;
    }) {
      return dd.query({
        db,
        name,
        key: key as Key,
        duration,
        start,
        end,
      });
    },
    list(
      db: Database,
      key: PartialKey<TKey>,
      opts?: {
        range?: { start: number; end: number };
        limit?: number;
        order?: "asc" | "desc";
      },
    ) {
      return dd.list(db, name, key as Key, opts);
    },
    find(
      db: Database,
      key: PartialKey<TKey>,
      start: number,
      end: number,
      duration: number,
      opts?: {
        limit?: number;
        order?: `${"sum" | "count" | "min" | "max"} ${"asc" | "desc"}`;
      },
    ) {
      return dd.find<TKey>({
        db,
        key: key as Key,
        name,
        start,
        end,
        duration,
        opts,
      });
    },
  };
};
