import { DDSketch } from "./ddsketch";
import { Database, type SQLQueryBindings } from "bun:sqlite";
import { outdent } from "outdent";
import { canonicalize, type JsonRecord } from "./canonicalize";

export { DDSketch };
export { KeyMapping, LogarithmicMapping, DenseStore } from "./ddsketch";

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

  function upsertFacets({
    db,
    name,
    key,
    facets,
  }: {
    db: Database;
    name: string;
    key: string;
    facets: Record<string, string | number>;
  }) {
    const entries = Object.entries(facets);

    if (entries.length === 0) {
      return;
    }

    let values: string[] = [];
    let params: SQLQueryBindings[] = [];

    for (const [k, v] of entries) {
      values.push("(?, ?, ?, ?, 1)");
      params.push(name, key, k, v);
    }

    db.query(
      `insert into facets (name, key, facet_name, facet_value, count) values ${values} on conflict do update set count = count + 1`,
    ).run(...params);
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

        create table if not exists facets (
            name text not null,
            key text not null,
            facet_name text not null,
            facet_value text not null,
            count integer not null default 1,
            primary key (name, key, facet_name, facet_value)
        ) without rowid;

        create table if not exists stats (
            name text not null,
            key text not null,
            val real not null,
            facet_name text not null,
            facet_value text not null,
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
            primary key (name, key, facet_name, facet_value)
        ) without rowid;

        create table if not exists stat_sketches (
            name text not null,
            key text not null,
            facet_name text not null,
            facet_value text not null,
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
            primary key (name, key, facet_name, facet_value, duration, start)
        ) without rowid;
    `);
  }

  function recordStats({
    db,
    name,
    key,
    val,
    facet,
    timestamp = Date.now(),
    intervals,
  }: {
    db: Database;
    name: string;
    key: string;
    timestamp: number;
    val: number | ((stat?: { value: number; recordedAt: number }) => number);
    facet?: {
      name: string;
      value: string | number;
    };
    intervals: number[];
  }) {
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
        SQLQueryBindings[]
      >(
        outdent`
          select val, recorded_at, facet_name, facet_value, sketch, min, max, count, sum from stats 
          where name = ? and key = ? and facet_name = ? and facet_value = ?;`,
      )
      .get(name, key, facet?.name ?? "", facet?.value ?? "");

    const now = timestamp;
    const { sketch: wasup, ..._stat } = stat ?? {};
    console.info("recording stat for", { name, key, facet, retrieved: _stat });

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

      if (facet !== undefined) {
      }

      db.query(
        `insert into stats (name, key, val, facet_name, facet_value, recorded_at, sketch, min, max, count, sum, p50, p90, p95, p99) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      ).run(
        name,
        key,
        next,
        facet?.name ?? "",
        facet?.value ?? "",
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
        dd.sketch(db, name, key, next, now, interval, facet);
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
      `update stats set val = ?, recorded_at = ?, sketch = ?, min = ?, max = ?, count = ?, sum = ?, p50 = ?, p90 = ?, p95 = ?, p99 = ? where name = ? and key = ? and facet_name = ? and facet_value = ?;`,
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
      key,
      facet?.name ?? "",
      facet?.value ?? "",
    );

    for (const interval of intervals) {
      dd.sketch(db, name, key, next, now, interval, facet);
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
    facets,
  }: {
    db: Database;
    name: string;
    key: string;
    val: number | ((stat?: { value: number; recordedAt: number }) => number);
    timestamp?: number;
    intervals?: number[];
    facets?: Record<string, string | number>;
  }) {
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
        [name: string, key: string]
      >(
        `select val, recorded_at, sketch, min, max, count, sum from stats where name = ? and key = ?;`,
      )
      .get(name, key);

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
    ).run(name, key, next, timestamp);

    // upsert all facet records
    if (facets !== undefined) {
      upsertFacets({ db, name, key, facets });
    }

    // record stats for each facet
    for (const [facetName, facetValue] of Object.entries(facets ?? {})) {
      recordStats({
        db,
        name,
        key,
        val,
        timestamp,
        intervals,
        facet: { name: facetName, value: facetValue },
      });
    }

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
    key: string,
    val: number,
    timestamp: number,
    interval: number,
    facet?: { name: string; value: string | number },
  ) {
    const start = Math.floor(timestamp / interval) * interval;
    const end = start + interval;

    let clause = outdent`
      where name = ? and key = ? and duration = ? and start = ?
    `;

    const params: SQLQueryBindings[] = [name, key, interval, start];

    if (facet !== undefined) {
      clause += " and facet_name = ? and facet_value = ? ";
      params.push(facet.name, facet.value);
    }

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
            select sketch, min, max, count, sum from stat_sketches ${clause};
        `,
      )
      .get(...params);

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
        insert into stat_sketches (name, key, facet_name, facet_value, duration, start, end, count, sum, min, max, p50, p90, p95, p99, sketch)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict (name, key, facet_name, facet_value, duration, start) do update set
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
      key,
      facet?.name ?? "",
      facet?.value ?? "",
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
    key: string,
    opts?: {
      range?: { start: number; end: number };
      limit?: number;
      order?: "asc" | "desc";
    },
  ) {
    let clause = "where name = ? and key = ?";
    let params: SQLQueryBindings[] = [name, key];

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
        typeof params
      >(`select val, recorded_at from events ${clause}`)
      .all(...params);

    return events.map((event) => ({
      value: event.val,
      recordedAt: Number(event.recorded_at),
    }));
  }

  export function get(
    db: Database,
    name: string,
    key: string,
    facet?: {
      name: string;
      value: string | number;
    },
  ) {
    const stat = db
      .query<
        {
          val: number;
          recorded_at: number | bigint;
          facet_name: string;
          facet_value: string;
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
          select val, recorded_at, facet_name, facet_value, min, max, count, sum, p50, p90, p95, p99 from stats
          where name = ? and key = ? and facet_name = ? and facet_value = ?;
        `,
      )
      .get(name, key, facet?.name ?? "", facet?.value ?? "");

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

  export function query(
    db: Database,
    name: string,
    key: string,
    duration: number,
    start: number,
    end: number,
    facet?: {
      name: string;
      value: string | number;
    },
  ) {
    const samples = [];

    const rows = db
      .query<
        {
          start: number | bigint;
          end: number | bigint;
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
          select start, end, count, sum, min, max, p50, p90, p95, p99, sketch
          from stat_sketches 
          where name = ? and key = ? and duration = ? and start >= ? and end <= ? and facet_name = ? and facet_value = ?
          order by start asc;
        `,
      )
      .all(
        name,
        key,
        duration,
        start,
        end,
        facet?.name ?? "",
        facet?.value ?? "",
      );

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

export interface StatinSchemaOption {
  key: string | JsonRecord;
  facets?: Record<string, string | number>;
}

export type StatinSchema = Record<string, StatinSchemaOption>;

type StatinName<Schema extends StatinSchema> = keyof Schema extends never
  ? string
  : keyof Schema & string;

type GenerateUnions<Object> = {
  [K in keyof Object]: {
    name: K;
    value: Object[K];
  };
}[keyof Object];

export class Statin<Schema extends StatinSchema = {}> {
  init(db: Database) {
    return dd.init(db);
  }

  private serializeKey(constituents: string | JsonRecord): string {
    return canonicalize(constituents);
  }

  record<Name extends StatinName<Schema>>({
    db,
    name,
    key: _key,
    val,
    timestamp,
    intervals,
    facets,
  }: {
    db: Database;
    name: Name;
    key: Name extends keyof Schema ? Schema[Name]["key"] : string;
    val: number | ((stat?: { value: number; recordedAt: number }) => number);
    timestamp?: number;
    intervals?: number[];
    facets?: Name extends keyof Schema
      ? Partial<Schema[Name]["facets"]>
      : Record<string, string | number>;
  }) {
    const key = this.serializeKey(_key);
    return dd.record({
      db,
      name,
      key,
      val,
      timestamp,
      intervals,
      facets: facets as Schema[Name]["facets"],
    });
  }

  sketch<Name extends StatinName<Schema>>({
    db,
    name,
    key: _key,
    val,
    timestamp,
    interval,
    facet,
  }: {
    db: Database;
    name: Name;
    key: Name extends keyof Schema ? Schema[Name]["key"] : string;
    val: number;
    timestamp: number;
    interval: number;
    facet?: Name extends keyof Schema
      ? GenerateUnions<Schema[Name]["facets"]>
      : {
          name: string;
          value: string | number;
        };
  }) {
    const key = this.serializeKey(_key);
    return dd.sketch(
      db,
      name,
      key,
      val,
      timestamp,
      interval,
      facet as { name: string; value: string | number },
    );
  }

  get<Name extends keyof Schema & string>(
    db: Database,
    name: Name,
    key: Name extends keyof Schema ? Schema[Name]["key"] : string,
    facet?: Name extends keyof Schema
      ? GenerateUnions<Schema[Name]["facets"]>
      : {
          name: string;
          value: string | number;
        },
  ) {
    return dd.get(
      db,
      name,
      this.serializeKey(key),
      facet as { name: string; value: string | number },
    );
  }

  query<Name extends StatinName<Schema>>({
    db,
    name,
    key,
    duration,
    start,
    end,
    facet,
  }: {
    db: Database;
    name: Name;
    key: Name extends keyof Schema ? Schema[Name]["key"] : string;
    duration: number;
    start: number;
    end: number;
    facet?: Name extends keyof Schema
      ? GenerateUnions<Schema[Name]["facets"]>
      : {
          name: string;
          value: string | number;
        };
  }) {
    return dd.query(
      db,
      name,
      this.serializeKey(key),
      duration,
      start,
      end,
      facet as { name: string; value: string | number },
    );
  }

  list<Name extends StatinName<Schema>>(
    db: Database,
    name: Name,
    key: Name extends keyof Schema ? Schema[Name]["key"] : string,
    opts?: {
      range?: { start: number; end: number };
      limit?: number;
      order?: "asc" | "desc";
    },
  ) {
    return dd.list(db, name, this.serializeKey(key), opts);
  }
}
