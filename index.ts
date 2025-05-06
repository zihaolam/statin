import { DDSketch } from "./ddsketch";
import { Database, type SQLQueryBindings } from "bun:sqlite";
import { outdent } from "outdent";
import {
  canonicalize,
  type JsonType,
  type NonNestedJsonRecord,
} from "./canonicalize";
import { generateKeyWhereClause } from "./generateKeyWhereClause";

export { DDSketch };
export { KeyMapping, LogarithmicMapping, DenseStore } from "./ddsketch";

type Key = JsonType;

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

  function recordStats({
    db,
    name,
    key,
    val,
    timestamp = Date.now(),
    intervals,
  }: {
    db: Database;
    name: string;
    key: Key;
    timestamp: number;
    val: number | ((stat?: { value: number; recordedAt: number }) => number);
    intervals: number[];
  }) {
    const keyWhere = generateKeyWhereClause(key);
    const stat = db
      .query<
        {
          val: number;
          key: string;
          recorded_at: number | bigint;
          sketch: Uint8Array;
          min: number;
          max: number;
          count: number;
          sum: number;
        },
        SQLQueryBindings[]
      >(
        outdent`
          select val, key, recorded_at, sketch, min, max, count, sum from stats 
          where name = ? and ${keyWhere.clause};`,
      )
      .get(name, ...keyWhere.params);

    const serializedKey = serializeKey(key);

    const now = timestamp;

    if (stat === null) {
      let next = val;
      if (typeof next === "function") {
        next = next();
      }

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
        value: Number(stat.val),
        recordedAt: Number(stat.recorded_at),
      });
    }

    stat.recorded_at = Number(stat.recorded_at);

    // If the timestamp is in the future, we can't record it.
    if (stat.recorded_at >= now) {
      throw new RangeError("Timestamp is in the past.");
    }

    const sketch = DDSketch.deserialize(stat.sketch);

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
      recordedAt: stat.recorded_at,
    };
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
    const serializedKey = serializeKey(key);
    const stat = db
      .query<
        {
          val: number;
          recorded_at: number | bigint;
          sketch: Uint8Array;
          min: number;
          max: number;
          count: number;
          sum: number;
        },
        [name: string, ...params: SQLQueryBindings[]]
      >(
        `select val, recorded_at, sketch, min, max, count, sum from stats where name = ? and ${keyWhere.clause};`,
      )
      .get(name, ...keyWhere.params);

    let next = val;
    if (typeof next === "function") {
      if (stat !== null) {
        next = next({
          value: Number(stat.val),
          recordedAt: Number(stat.recorded_at),
        });
      } else {
        next = next();
      }
    }

    db.query(
      `insert into events (name, key, val, recorded_at) values (?, ?, ?, ?)`,
    ).run(name, serializedKey, next, timestamp);

    return recordStats({
      db,
      name,
      key,
      val,
      timestamp,
      intervals,
    });
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

  export function get({
    db,
    name,
    key,
  }: {
    db: Database;
    name: string;
    key: Key;
  }) {
    const keyWhere = generateKeyWhereClause(key);
    const stat = db
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
        },
        SQLQueryBindings[]
      >(
        outdent`
          select val, recorded_at, min, max, count, sum, p50, p90, p95, p99 from stats
          where name = ? and ${keyWhere.clause};
        `,
      )
      .get(name, ...keyWhere.params);

    if (stat === null) {
      return null;
    }

    return {
      value: stat.val,
      recordedAt: Number(stat.recorded_at),
      stat: {
        min: stat.min,
        max: stat.max,
        count: stat.count,
        sum: stat.sum,
        p50: stat.p50,
        p90: stat.p90,
        p95: stat.p95,
        p99: stat.p99,
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
    const samples = [];
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

      samples.push({
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
      samples,
    };
  }
}

export type StatinSchema = Record<string, Key>;

type StatinName<Schema extends StatinSchema> = keyof Schema extends never
  ? string
  : keyof Schema & string;

export class Statin<Schema extends StatinSchema = {}> {
  init(db: Database) {
    return dd.init(db);
  }

  record<Name extends StatinName<Schema>>({
    db,
    name,
    key,
    val,
    timestamp,
    intervals,
  }: {
    db: Database;
    name: Name;
    key: Name extends keyof Schema ? Schema[Name] : string;
    val: number | ((stat?: { value: number; recordedAt: number }) => number);
    timestamp?: number;
    intervals?: number[];
  }) {
    return dd.record({
      db,
      name,
      key,
      val,
      timestamp,
      intervals,
    });
  }

  sketch<Name extends StatinName<Schema>>({
    db,
    name,
    key,
    val,
    timestamp,
    interval,
  }: {
    db: Database;
    name: Name;
    key: Name extends keyof Schema ? Schema[Name] : string;
    val: number;
    timestamp: number;
    interval: number;
  }) {
    return dd.sketch(db, name, key, val, timestamp, interval);
  }

  get<Name extends keyof Schema & string>({
    db,
    name,
    key,
  }: {
    db: Database;
    name: Name;
    key: Name extends keyof Schema ? Schema[Name] : string;
  }) {
    return dd.get({
      db,
      name,
      key,
    });
  }

  query<Name extends StatinName<Schema>>({
    db,
    name,
    key,
    duration,
    start,
    end,
  }: {
    db: Database;
    name: Name;
    key: Name extends keyof Schema ? Schema[Name] : string;
    duration: number;
    start: number;
    end: number;
  }) {
    return dd.query({
      db,
      name,
      key,
      duration,
      start,
      end,
    });
  }

  list<Name extends StatinName<Schema>>(
    db: Database,
    name: Name,
    key: Name extends keyof Schema ? Schema[Name] : string,
    opts?: {
      range?: { start: number; end: number };
      limit?: number;
      order?: "asc" | "desc";
    },
  ) {
    return dd.list(db, name, key, opts);
  }
}
