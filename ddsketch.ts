// A simple, correct TypeScript implementation of DDSketch.
// This implementation is based off of the reference Go, Python, and JavaScript implementations:
// - https://github.com/DataDog/sketches-go
// - https://github.com/DataDog/sketches-py
// - https://github.com/DataDog/sketches-js

const MAX_SAFE_FLOAT = Number.MAX_VALUE;
const MIN_SAFE_FLOAT = Math.pow(2, -1023);

export class DDSketch {
  map: LogarithmicMapping;
  positives: DenseStore;
  negatives: DenseStore;
  zeroCount: number = 0.0;

  constructor(map = new LogarithmicMapping(0.01)) {
    this.map = map;
    this.positives = new DenseStore();
    this.negatives = new DenseStore();
  }

  public get byteLength() {
    return (
      this.map.byteLength +
      this.positives.byteLength +
      this.negatives.byteLength +
      8
    );
  }

  public serialize() {
    // map (16 bytes)
    // positives (serialized dense store)
    // negatives (serialized dense store)
    // zero count (8 bytes)
    const data = new Uint8Array(this.byteLength);
    data.set(this.map.serialize(), 0);
    data.set(this.positives.serialize(), this.map.byteLength);
    data.set(
      new Uint8Array(this.negatives.serialize()),
      this.map.byteLength + this.positives.byteLength
    );
    new DataView(data.buffer, data.byteOffset, data.byteLength).setFloat64(
      this.map.byteLength +
        this.positives.byteLength +
        this.negatives.byteLength,
      this.zeroCount
    );
    return new Uint8Array(data);
  }

  public static deserialize(data: Uint8Array) {
    const map = LogarithmicMapping.deserialize(data);
    const positives = DenseStore.deserialize(data.slice(map.byteLength));
    const negatives = DenseStore.deserialize(
      data.slice(map.byteLength + positives.byteLength)
    );
    const zeroCount = new DataView(
      data.buffer,
      data.byteOffset,
      data.byteLength
    ).getFloat64(map.byteLength + positives.byteLength + negatives.byteLength);

    const sketch = new DDSketch(map);
    sketch.positives = positives;
    sketch.negatives = negatives;
    sketch.zeroCount = zeroCount;
    return sketch;
  }

  public add(value: number, count: number = 1.0) {
    if (count < 0) {
      throw new Error("Can't add a negative count");
    }
    if (value > this.map.minPossible) {
      this.positives.add(this.map.key(value), count);
    } else if (value < -this.map.minPossible) {
      this.negatives.add(this.map.key(-value), count);
    } else {
      this.zeroCount += count;
    }
  }

  public clear() {
    this.positives.clear();
    this.negatives.clear();
    this.zeroCount = 0.0;
  }

  public get count() {
    return this.zeroCount + this.positives.count + this.negatives.count;
  }

  public get sum() {
    let sum = 0.0;
    for (const [value, count] of this) {
      sum += value * count;
    }
    return sum;
  }

  public get max() {
    if (this.positives.count !== 0) {
      return this.map.value(this.positives.maxKey);
    }
    if (this.zeroCount > 0) {
      return 0;
    }
    if (this.negatives.count !== 0) {
      return -this.map.value(this.negatives.minKey);
    }
    return NaN;
  }

  public get min() {
    if (this.negatives.count !== 0) {
      return -this.map.value(this.negatives.maxKey);
    }
    if (this.zeroCount > 0) {
      return 0;
    }
    if (this.positives.count !== 0) {
      return this.map.value(this.positives.minKey);
    }
    return NaN;
  }

  *[Symbol.iterator]() {
    if (this.zeroCount !== 0) {
      yield [0, this.zeroCount] as const;
    }
    for (const [key, count] of this.positives) {
      yield [this.map.value(key), count] as const;
    }
    for (const [key, count] of this.negatives) {
      yield [-this.map.value(key), count] as const;
    }
  }

  public merge(other: DDSketch) {
    if (this.map.gamma !== other.map.gamma) {
      throw new Error("Can't merge sketches with different gamma values");
    }
    this.positives.merge(other.positives);
    this.negatives.merge(other.negatives);
    this.zeroCount += other.zeroCount;
  }

  public copy(other: DDSketch) {
    this.map = other.map;
    this.positives.copy(other.positives);
    this.negatives.copy(other.negatives);
    this.zeroCount = other.zeroCount;
  }

  public getValueAtQuantile(
    quantile: number,
    stats: { count: number } = { count: this.count }
  ) {
    if (quantile < 0 || quantile > 1 || stats.count === 0) {
      return NaN;
    }

    const rank = quantile * (stats.count - 1);

    if (rank < this.negatives.count) {
      const reversedRank = this.negatives.count - rank - 1;
      const key = this.negatives.keyAtRank(reversedRank, false);
      return -this.map.value(key);
    } else if (rank < this.zeroCount + this.negatives.count) {
      return 0;
    } else {
      const key = this.positives.keyAtRank(
        rank - this.zeroCount - this.negatives.count
      );
      return this.map.value(key);
    }
  }
}

export class DenseStore {
  bins = new Float64Array();
  count = 0;
  offset = 0;
  minKey = Infinity;
  maxKey = -Infinity;

  public get byteLength() {
    return 4 + 4 + this.bins.length * 8;
  }

  public serialize() {
    // offset (4 bytes)
    // bins.length (4 bytes)
    // [...bins] (8 * bins.length bytes)
    const data = new ArrayBuffer(this.byteLength);
    const view = new DataView(data);
    view.setInt32(0, this.offset);
    view.setUint32(4, this.bins.length);
    for (let i = 0; i < this.bins.length; i++) {
      view.setFloat64(8 + i * 8, this.bins[i]);
    }
    return new Uint8Array(data);
  }

  public static deserialize(data: Uint8Array) {
    const store = new DenseStore();
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

    const offset = view.getInt32(0);

    store.offset = offset;

    const numBins = view.getUint32(4);

    for (let i = 0; i < numBins; i++) {
      const bin = view.getFloat64(8 + i * 8);
      store.add(offset + i, bin);
    }

    return store;
  }

  public add(key: number, count: number = 1) {
    if (count === 0) {
      return;
    }
    const index = this.normalize(key);
    this.bins[index] += count;
    this.count += count;
  }

  public keyAtRank(rank: number, lower: boolean = true) {
    rank = Math.max(0, rank);

    let n = 0;
    for (const [i, b] of this.bins.entries()) {
      n += b;
      if ((lower && n > rank) || (!lower && n >= rank + 1)) {
        return i + this.offset;
      }
    }

    return this.maxKey;
  }

  public merge(other: DenseStore) {
    if (other.count === 0) {
      return;
    }

    if (this.count === 0) {
      this.copy(other);
      return;
    }

    if (other.minKey < this.minKey || other.maxKey > this.maxKey) {
      this.extendRange(other.minKey, other.maxKey);
    }

    for (let key = other.minKey; key <= other.maxKey; key++) {
      this.bins[key - this.offset] += other.bins[key - other.offset];
    }

    this.count += other.count;
  }

  public copy(other: DenseStore) {
    this.bins = new Float64Array(other.bins.length);
    this.bins.set(other.bins);
    this.count = other.count;
    this.offset = other.offset;
    this.minKey = other.minKey;
    this.maxKey = other.maxKey;
  }

  public clear() {
    this.bins = this.bins.slice(0, 0);
    this.count = 0;
    this.minKey = Infinity;
    this.maxKey = -Infinity;
  }

  public reweigh(w: number) {
    if (w <= 0) {
      throw new Error("Can't reweigh by a negative factor");
    }
    if (w === 1) {
      return;
    }
    this.count *= w;
    for (let key = this.minKey; key <= this.maxKey; key++) {
      this.bins[key - this.offset] *= w;
    }
  }

  public *[Symbol.iterator]() {
    for (let key = this.minKey; key <= this.maxKey; key++) {
      if (this.bins[key - this.offset] > 0) {
        yield [key, this.bins[key - this.offset]] as const;
      }
    }
  }

  private normalize(key: number) {
    if (key < this.minKey || key > this.maxKey) {
      this.extendRange(key, key);
    }
    return key - this.offset;
  }

  private getNewLength(newMinKey: number, newMaxKey: number) {
    const desiredLength = newMaxKey - newMinKey + 1;
    return 128 * Math.ceil(desiredLength / 128);
  }

  private extendRange(newMinKey: number, newMaxKey: number) {
    newMinKey = Math.min(newMinKey, this.minKey);
    newMaxKey = Math.max(newMaxKey, this.maxKey);

    if (this.count === 0) {
      const initialLength = this.getNewLength(newMinKey, newMaxKey);
      this.bins = new Float64Array(initialLength);
      this.offset = newMinKey;
      this.adjust(newMinKey, newMaxKey);
    } else if (
      newMinKey >= this.minKey &&
      newMaxKey < this.offset + this.bins.length
    ) {
      this.minKey = newMinKey;
      this.maxKey = newMaxKey;
    } else {
      const newLength = this.getNewLength(newMinKey, newMaxKey);
      if (newLength > this.bins.length) {
        const newbins = new Float64Array(newLength);
        newbins.set(this.bins);
        this.bins = newbins;
      }
      this.adjust(newMinKey, newMaxKey);
    }
  }

  private adjust(newMinKey: number, newMaxKey: number) {
    const midIndex = newMinKey + Math.floor((newMaxKey - newMinKey + 1) / 2);
    this.shiftBins(this.offset + Math.floor(this.bins.length / 2) - midIndex);
    this.minKey = newMinKey;
    this.maxKey = newMaxKey;
  }

  private shiftBins(shift: number) {
    if (shift === 0) return;

    const newBins = new Float64Array(this.bins.length);

    if (shift > 0) {
      newBins.set(this.bins.slice(0, -shift), shift);
    } else {
      newBins.set(this.bins.slice(-shift));
    }

    this.bins = newBins;
    this.offset -= shift;
  }
}

export abstract class KeyMapping {
  offset: number;
  gamma: number;
  multiplier: number;
  minPossible: number;
  maxPossible: number;

  constructor(relativeAccuracy: number, offset: number = 0.0) {
    if (relativeAccuracy <= 0 || relativeAccuracy >= 1) {
      throw new Error("Relative accuracy must be between 0 and 1");
    }

    const mantissa = (2 * relativeAccuracy) / (1 - relativeAccuracy);

    this.offset = offset;
    this.gamma = 1 + mantissa;
    this.multiplier = 1 / Math.log1p(mantissa);
    this.minPossible = MIN_SAFE_FLOAT * this.gamma;
    this.maxPossible = MAX_SAFE_FLOAT / this.gamma;
  }

  abstract logGamma(value: number): number;
  abstract powGamma(value: number): number;

  public key(value: number) {
    return Math.ceil(this.logGamma(value)) + this.offset;
  }

  public value(key: number) {
    return this.powGamma(key - this.offset) * (2 / (1 + this.gamma));
  }
}

export class LogarithmicMapping extends KeyMapping {
  constructor(relativeAccuracy: number, offset: number = 0.0) {
    super(relativeAccuracy, offset);
    this.multiplier *= Math.log(2);
  }

  public static fromGammaOffset(gamma: number, offset: number) {
    const relativeAccuracy = (gamma - 1) / (gamma + 1);
    return new LogarithmicMapping(relativeAccuracy, offset);
  }

  public get byteLength() {
    return 16;
  }

  public serialize() {
    // gamma (8 bytes)
    // index offset (8 bytes)
    const data = new ArrayBuffer(16);
    const view = new DataView(data);
    view.setFloat64(0, this.gamma);
    view.setFloat64(8, this.offset);
    return new Uint8Array(data);
  }

  public static deserialize(data: Uint8Array) {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const gamma = view.getFloat64(0);
    const offset = view.getFloat64(8);
    return LogarithmicMapping.fromGammaOffset(gamma, offset);
  }

  public logGamma(value: number) {
    return Math.log2(value) * this.multiplier;
  }

  public powGamma(value: number) {
    return Math.pow(2, value / this.multiplier);
  }
}
