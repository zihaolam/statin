import { type SQLQueryBindings } from "bun:sqlite";
import { type JsonType } from "./canonicalize";

export type Key = JsonType;

export function generateKeyWhereClause(
  key: JsonType,
  namespace = "",
): { clause: string; keyFields: string[]; params: SQLQueryBindings[] } {
  const clauses: string[] = [];
  const params: SQLQueryBindings[] = [];
  const keyFields: string[] = [];

  // Helper to compute the JSON path we’ll inject into json_extract()
  const path = namespace || "$";

  // 1) Primitive (string|number|boolean|null)
  if (
    key === null ||
    typeof key === "string" ||
    typeof key === "number" ||
    typeof key === "boolean"
  ) {
    if (namespace === "") {
      // top-level column equality
      clauses.push(`key = ?`);
      params.push(JSON.stringify(key));
    } else {
      // nested JSON value
      clauses.push(`json_extract(key, '${path}') = ?`);
      keyFields.push(`json_extract(key, '${path}')`);
      params.push(key);
    }
    return { clause: clauses.join(" and "), keyFields, params };
  }

  // 2) Array
  if (Array.isArray(key)) {
    if (key.length === 0) {
      clauses.push(`json_type(key) = 'array'`);
      // special case: empty array
      if (path === "$") {
        clauses.push(`json_array_length(key) = 0`);
        keyFields.push(`json_array_length(key)`);
      } else {
        clauses.push(`json_array_length(json_extract(key, '${path}')) = 0`);
        keyFields.push(`json_array_length(json_extract(key, '${path}'))`);
      }

      return { clause: clauses.join(" and "), keyFields, params };
    }
    for (const [idx, item] of key.entries()) {
      const childPath = `${path}[${idx}]`;
      const sub = generateKeyWhereClause(item, childPath);
      clauses.push(sub.clause);
      params.push(...sub.params);
      keyFields.push(...sub.keyFields);
    }
    return { clause: clauses.join(" and "), keyFields, params };
  }

  // 3) Object
  const entries = Object.entries(key);
  if (entries.length === 0) {
    if (path === "$") {
      return {
        clause: "key = ?",
        keyFields: ["key"],
        params: ["{}"],
      };
    }

    // special case: empty object
    return {
      clause: `json_extract(key, ${path}) = ?`,
      keyFields: [`json_extract(key, ${path})`],
      params: ["{}"],
    };
  }
  for (const [k, v] of entries) {
    const childPath = `${path}.${k}`;
    const sub = generateKeyWhereClause(v, childPath);
    clauses.push(sub.clause);
    params.push(...sub.params);
    keyFields.push(...sub.keyFields);
  }
  return { clause: clauses.join(" and "), keyFields, params };
}
