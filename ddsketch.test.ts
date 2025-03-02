import { expect, describe, test } from "bun:test";
import { DDSketch } from "./ddsketch";

describe("DDSketch", () => {
  // https://github.com/DataDog/sketches-js/issues/22
  test("quantile values from merged positive/negative-valued sketches are equal", () => {
    // Test data.
    const posA = [1, 2, 3, 4, 5];
    const posB = [6, 7, 8, 9, 10];

    // These two will each get half the positive data.
    const ddsPosA = new DDSketch();
    const ddsPosB = new DDSketch();

    // And then merge into this one.
    const ddsPosAB = new DDSketch();

    // Whereas this one reads all the positive data sequentially.
    const ddsPosC = new DDSketch();

    for (const s of posA) {
      ddsPosA.add(s);
      ddsPosC.add(s);
    }

    for (const s of posB) {
      ddsPosB.add(s);
      ddsPosC.add(s);
    }

    ddsPosAB.copy(ddsPosA);
    ddsPosAB.merge(ddsPosB);

    // We'll repeat this for negative numbers.
    const negA = [-10, -9, -8, -7, -6];
    const negB = [-5, -4, -3, -2, -1];
    const ddsNegA = new DDSketch();
    const ddsNegB = new DDSketch();
    const ddsNegAB = new DDSketch();
    const ddsNegC = new DDSketch();

    for (const s of negA) {
      ddsNegA.add(s);
      ddsNegC.add(s);
    }

    for (const s of negB) {
      ddsNegB.add(s);
      ddsNegC.add(s);
    }

    ddsNegAB.copy(ddsNegA);
    ddsNegAB.merge(ddsNegB);

    // The percentiles should be the same for the sequential c sketches
    // and the merged ab sketches.

    const expected = [
      { q: 0.01, pqv: 0.99, nqv: -10.0747 },
      { q: 0.25, pqv: 2.97423, nqv: -7.92497 },
      { q: 0.5, pqv: 5.00283, nqv: -5.98951 },
      { q: 0.75, pqv: 7.02879, nqv: -4.01484 },
      { q: 0.99, pqv: 8.93542, nqv: -1.99366 },
    ];

    // for (const { q } of expected) {
    //   console.log(
    //     `pq: ${q.toFixed(2)}\tc: ${ddsPosC
    //       .getValueAtQuantile(q)
    //       .toFixed(5)}\tab: ${ddsPosAB.getValueAtQuantile(q).toFixed(5)}`
    //   );
    // }

    // for (const { q } of expected) {
    //   console.log(
    //     `nq: ${q.toFixed(2)}\tc: ${ddsNegC
    //       .getValueAtQuantile(q)
    //       .toFixed(5)}\tab: ${ddsNegAB.getValueAtQuantile(q).toFixed(5)}`
    //   );
    // }

    for (const { q, pqv, nqv } of expected) {
      expect(ddsPosC.getValueAtQuantile(q)).toBeCloseTo(pqv);
      expect(ddsPosC.getValueAtQuantile(q)).toBeCloseTo(
        ddsPosAB.getValueAtQuantile(q)
      );
      expect(ddsNegC.getValueAtQuantile(q)).toBeCloseTo(nqv);
      expect(ddsNegC.getValueAtQuantile(q)).toBeCloseTo(
        ddsNegAB.getValueAtQuantile(q)
      );
    }

    const serializedPosC = ddsPosC.serialize();
    const serializedNegC = ddsNegC.serialize();
    const serializedPosAB = ddsPosAB.serialize();
    const serializedNegAB = ddsNegAB.serialize();

    const deserializedPosC = DDSketch.deserialize(serializedPosC);
    const deserializedNegC = DDSketch.deserialize(serializedNegC);
    const deserializedPosAB = DDSketch.deserialize(serializedPosAB);
    const deserializedNegAB = DDSketch.deserialize(serializedNegAB);

    for (const { q, pqv, nqv } of expected) {
      expect(deserializedPosAB.getValueAtQuantile(q)).toBeCloseTo(pqv);
      expect(deserializedPosC.getValueAtQuantile(q)).toBeCloseTo(pqv);
      expect(deserializedPosC.getValueAtQuantile(q)).toBeCloseTo(
        ddsPosAB.getValueAtQuantile(q)
      );
      expect(deserializedNegAB.getValueAtQuantile(q)).toBeCloseTo(nqv);
      expect(deserializedNegC.getValueAtQuantile(q)).toBeCloseTo(nqv);
      expect(deserializedNegC.getValueAtQuantile(q)).toBeCloseTo(
        ddsNegAB.getValueAtQuantile(q)
      );
    }

    expect(deserializedPosC.min).toBeCloseTo(ddsPosC.min);
    expect(deserializedPosC.max).toBeCloseTo(ddsPosC.max);
    expect(deserializedPosC.count).toBeCloseTo(ddsPosC.count);
    expect(deserializedPosC.sum).toBeCloseTo(ddsPosC.sum);

    expect(deserializedNegC.min).toBeCloseTo(ddsNegC.min);
    expect(deserializedNegC.max).toBeCloseTo(ddsNegC.max);
    expect(deserializedNegC.count).toBeCloseTo(ddsNegC.count);
    expect(deserializedNegC.sum).toBeCloseTo(ddsNegC.sum);

    expect(deserializedPosAB.min).toBeCloseTo(ddsPosAB.min);
    expect(deserializedPosAB.max).toBeCloseTo(ddsPosAB.max);
    expect(deserializedPosAB.count).toBeCloseTo(ddsPosAB.count);
    expect(deserializedPosAB.sum).toBeCloseTo(ddsPosAB.sum);

    expect(deserializedNegAB.min).toBeCloseTo(ddsNegAB.min);
    expect(deserializedNegAB.max).toBeCloseTo(ddsNegAB.max);
    expect(deserializedNegAB.count).toBeCloseTo(ddsNegAB.count);
    expect(deserializedNegAB.sum).toBeCloseTo(ddsNegAB.sum);
  });
});
