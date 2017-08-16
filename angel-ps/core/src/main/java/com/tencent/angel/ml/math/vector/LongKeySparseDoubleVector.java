package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.VectorType;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Sparse double vector with long key.
 */
public class LongKeySparseDoubleVector extends LongKeyDoubleVector {
  /** A (long->double) map*/
  private final Long2DoubleOpenHashMap indexToValueMap;

  public static final int INIT_SIZE = 65536;

  /**
   * Init the empty vector
   */
  public LongKeySparseDoubleVector() {
    super(Long.MAX_VALUE);
    this.indexToValueMap = new Long2DoubleOpenHashMap(INIT_SIZE);
  }

  /**
   * Init the vector by setting the dimension
   *
   * @param dim vector dimension
   */
  public LongKeySparseDoubleVector(long dim) {
    this(dim, INIT_SIZE);
  }

  /**
   * Init the dim and capacity for vector
   *
   * @param dim      vector dimension
   * @param capacity map initialization size
   */
  public LongKeySparseDoubleVector(long dim, int capacity) {
    super(dim);
    this.indexToValueMap = new Long2DoubleOpenHashMap(capacity);
  }

  /**
   * Init the vector by setting the dimension , indexes and values
   *
   * @param dim     vector dimension
   * @param indexes value indexes
   * @param values  values
   */
  public LongKeySparseDoubleVector(long dim, long[] indexes, double[] values) {
    super(dim);
    assert indexes.length == values.length;
    this.indexToValueMap = new Long2DoubleOpenHashMap(indexes, values);
  }

  /**
   * Init the vector by setting the dimension
   *
   * @param dim vector dimension
   * @param map a (long->double) map
   */
  public LongKeySparseDoubleVector(long dim, Long2DoubleOpenHashMap map) {
    super(dim);
    this.indexToValueMap = map;
  }

  /**
   * Init the vector by another vector
   *
   * @param other other vector
   */
  public LongKeySparseDoubleVector(LongKeySparseDoubleVector other) {
    super(other.getLongDim());
    this.matrixId = other.matrixId;
    this.rowId = other.rowId;
    this.clock = other.clock;
    this.indexToValueMap = new Long2DoubleOpenHashMap(other.indexToValueMap);
  }

  @Override public TVector plusBy(TAbstractVector other) {
    if (other instanceof LongKeySparseDoubleVector)
      return plusBy((LongKeySparseDoubleVector) other);

    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private LongKeySparseDoubleVector plusBy(LongKeySparseDoubleVector other) {
    assert dim == other.dim;
    ObjectIterator<Long2DoubleMap.Entry> iter =
      other.indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      indexToValueMap.addTo(entry.getLongKey(), entry.getDoubleValue());
    }

    return this;
  }

  @Override public TVector plusBy(long index, double x) {
    indexToValueMap.addTo(index, x);
    return this;
  }

  @Override public TVector plusBy(TAbstractVector other, double x) {
    if (other instanceof LongKeySparseDoubleVector)
      return plusBy((LongKeySparseDoubleVector) other, x);

    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private LongKeySparseDoubleVector plusBy(LongKeySparseDoubleVector other, double x) {
    assert dim == other.dim;
    ObjectIterator<Long2DoubleMap.Entry> iter =
      other.indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      indexToValueMap.addTo(entry.getLongKey(), entry.getDoubleValue() * x);
    }

    return this;
  }

  @Override public TVector plus(TAbstractVector other) {
    if (other instanceof LongKeySparseDoubleVector)
      return plus((LongKeySparseDoubleVector) other);

    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plus " + other.getClass()
        .getName());
  }

  private LongKeySparseDoubleVector plus(LongKeySparseDoubleVector other) {
    assert dim == other.dim;
    LongKeySparseDoubleVector baseVector = null;
    LongKeySparseDoubleVector streamVector = null;
    if (size() < other.size()) {
      baseVector = new LongKeySparseDoubleVector(other);
      streamVector = this;
    } else {
      baseVector = new LongKeySparseDoubleVector(this);
      streamVector = other;
    }

    ObjectIterator<Long2DoubleMap.Entry> iter =
      streamVector.indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      baseVector.indexToValueMap.addTo(entry.getLongKey(), entry.getDoubleValue());
    }

    return baseVector;
  }

  @Override public TVector plus(TAbstractVector other, double x) {
    if (other instanceof LongKeySparseDoubleVector)
      return plus((LongKeySparseDoubleVector) other, x);

    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plus " + other.getClass()
        .getName());
  }

  private LongKeySparseDoubleVector plus(LongKeySparseDoubleVector other, double x) {
    assert dim == other.dim;
    LongKeySparseDoubleVector baseVector = null;
    LongKeySparseDoubleVector streamVector = null;
    if (size() < other.size()) {
      baseVector = new LongKeySparseDoubleVector(other);
      streamVector = this;
    } else {
      baseVector = new LongKeySparseDoubleVector(this);
      streamVector = other;
    }

    ObjectIterator<Long2DoubleMap.Entry> iter =
      streamVector.indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      baseVector.indexToValueMap.addTo(entry.getLongKey(), entry.getDoubleValue() * x);
    }

    return baseVector;
  }

  @Override public double dot(TAbstractVector other) {
    if (other instanceof LongKeySparseDoubleVector)
      return dot((LongKeySparseDoubleVector) other);

    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " dot " + other.getClass()
        .getName());
  }

  private double dot(LongKeySparseDoubleVector other) {
    assert dim == other.dim;
    double ret = 0.0;
    if (size() < other.size()) {
      ObjectIterator<Long2DoubleMap.Entry> iter =
        indexToValueMap.long2DoubleEntrySet().fastIterator();
      Long2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        ret += other.get(entry.getLongKey()) * entry.getDoubleValue();
      }
      return ret;
    } else {
      return other.dot(this);
    }
  }

  @Override public double get(long key) {
    return indexToValueMap.get(key);
  }

  @Override public TVector set(long key, double value) {
    indexToValueMap.put(key, value);
    return this;
  }

  @Override public TVector times(double x) {
    LongKeySparseDoubleVector vector = new LongKeySparseDoubleVector(dim, indexToValueMap.size());
    ObjectIterator<Long2DoubleMap.Entry> iter =
      indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.set(entry.getLongKey(), entry.getDoubleValue() * x);
    }
    return vector;
  }

  @Override public TVector timesBy(double x) {
    ObjectIterator<Long2DoubleMap.Entry> iter =
      indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      entry.setValue(entry.getDoubleValue() * x);
    }
    return this;
  }

  @Override public TVector filter(double x) {
    LongKeySparseDoubleVector vector = new LongKeySparseDoubleVector(this.dim);

    ObjectIterator<Long2DoubleMap.Entry> iter =
      indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      double value = entry.getDoubleValue();
      if (Math.abs(value) > x) {
        vector.set(entry.getLongKey(), value);
      }
    }
    vector.setRowId(rowId).setMatrixId(matrixId).setClock(clock);
    return vector;
  }

  @Override public TVector clone() {
    return new LongKeySparseDoubleVector(this);
  }

  @Override public void clear() {
    indexToValueMap.clear();
  }

  @Override public long nonZeroNumber() {
    long counter = 0L;
    ObjectIterator<Long2DoubleMap.Entry> iter =
      indexToValueMap.long2DoubleEntrySet().fastIterator();
    while (iter.hasNext()) {
      if(iter.next().getDoubleValue() > 0)
        counter++;
    }

    return counter;
  }

  @Override public void clone(TVector vector) {
    assert vector instanceof LongKeySparseDoubleVector;

    this.matrixId = ((LongKeySparseDoubleVector)vector).matrixId;
    this.rowId = ((LongKeySparseDoubleVector)vector).rowId;
    this.clock = ((LongKeySparseDoubleVector)vector).clock;
    this.indexToValueMap.clear();
    this.indexToValueMap.putAll(((LongKeySparseDoubleVector)vector).indexToValueMap);
  }

  @Override public double squaredNorm() {
    ObjectIterator<Long2DoubleMap.Entry> iter = indexToValueMap.long2DoubleEntrySet().iterator();
    double sum = 0;
    while (iter.hasNext()) {
      double v = iter.next().getDoubleValue();
      sum += v * v;
    }
    return sum;
  }

  @Override public double sparsity() {
    return nonZeroNumber() / dim;
  }

  @Override public VectorType getType() {
    return VectorType.T_DOUBLE_SPARSE_LONGKEY;
  }

  @Override public int size() {
    return indexToValueMap.size();
  }
}
