package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.VectorType;
import com.tencent.angel.protobuf.generated.MLProtos;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Sparse double vector with long key.
 */
public class SparseDoubleLongKeyVector extends LongKeyDoubleVector {
  private static final Log LOG = LogFactory.getLog(SparseDoubleLongKeyVector.class);
  /** A (long->double) map */
  private volatile Long2DoubleOpenHashMap indexToValueMap;

  public static final int INIT_SIZE = 1024 * 1024;

  /**
   * Init the empty vector
   */
  public SparseDoubleLongKeyVector() {
    this(-1, -1);
  }

  /**
   * Init the vector by setting the dimension
   *
   * @param dim vector dimension
   */
  public SparseDoubleLongKeyVector(long dim) {
    this(dim, -1);
  }

  /**
   * Init the dim and capacity for vector
   *
   * @param dim      vector dimension
   * @param capacity map initialization size
   */
  public SparseDoubleLongKeyVector(long dim, int capacity) {
    super(dim);
    if(capacity <= 0) {
      this.indexToValueMap = new Long2DoubleOpenHashMap(INIT_SIZE);
    } else {
      this.indexToValueMap = new Long2DoubleOpenHashMap(capacity);
    }
  }

  /**
   * Init the vector by setting the dimension , indexes and values
   *
   * @param dim     vector dimension
   * @param indexes value indexes
   * @param values  values
   */
  public SparseDoubleLongKeyVector(long dim, long[] indexes, double[] values) {
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
  public SparseDoubleLongKeyVector(long dim, Long2DoubleOpenHashMap map) {
    super(dim);
    this.indexToValueMap = map;
  }

  /**
   * Init the vector by another vector
   *
   * @param other other vector
   */
  public SparseDoubleLongKeyVector(SparseDoubleLongKeyVector other) {
    super(other.getLongDim());
    this.matrixId = other.matrixId;
    this.rowId = other.rowId;
    this.clock = other.clock;
    this.indexToValueMap = new Long2DoubleOpenHashMap(other.indexToValueMap);
  }

  @Override public TVector plusBy(TAbstractVector other) {
    if (other instanceof SparseDoubleLongKeyVector)
      return plusBy((SparseDoubleLongKeyVector) other);

    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private SparseDoubleLongKeyVector plusBy(SparseDoubleLongKeyVector other) {
    assert dim == other.dim;
    LOG.debug("other vector size=" + other.size() + ", this vector size=" + this.size());
    long startTs = System.currentTimeMillis();

    if(indexToValueMap.size() == 0) {
      indexToValueMap.putAll(other.indexToValueMap);
    } else if(indexToValueMap.size() < other.size()) {
      Long2DoubleOpenHashMap oldMap = indexToValueMap;
      indexToValueMap = other.indexToValueMap.clone();

      ObjectIterator<Long2DoubleMap.Entry> iter =
        oldMap.long2DoubleEntrySet().fastIterator();
      Long2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        indexToValueMap.addTo(entry.getLongKey(), entry.getDoubleValue());
      }
    } else {
      ObjectIterator<Long2DoubleMap.Entry> iter =
        other.indexToValueMap.long2DoubleEntrySet().fastIterator();
      Long2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        indexToValueMap.addTo(entry.getLongKey(), entry.getDoubleValue());
      }
    }

    LOG.debug("plusBy use time=" + (System.currentTimeMillis() - startTs));
    return this;
  }

  private double sum(SparseDoubleLongKeyVector row) {
    double [] data = row.getValues();
    double ret = 0.0;
    for(int i = 0; i < data.length; i++) {
      ret += data[i];
    }

    return ret;
  }

  @Override public TVector plusBy(long index, double x) {
    indexToValueMap.addTo(index, x);
    return this;
  }

  @Override public TVector plusBy(TAbstractVector other, double x) {
    if (other instanceof SparseDoubleLongKeyVector)
      return plusBy((SparseDoubleLongKeyVector) other, x);

    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private SparseDoubleLongKeyVector plusBy(SparseDoubleLongKeyVector other, double x) {
    assert dim == other.dim;
    if(indexToValueMap.size() < other.size()) {
      Long2DoubleOpenHashMap oldMap = indexToValueMap;
      indexToValueMap = new Long2DoubleOpenHashMap(other.size());
      indexToValueMap.putAll(oldMap);
    }

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
    if (other instanceof SparseDoubleLongKeyVector)
      return plus((SparseDoubleLongKeyVector) other);

    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plus " + other.getClass()
        .getName());
  }

  private SparseDoubleLongKeyVector plus(SparseDoubleLongKeyVector other) {
    assert dim == other.dim;
    SparseDoubleLongKeyVector baseVector = null;
    SparseDoubleLongKeyVector streamVector = null;
    if (size() < other.size()) {
      baseVector = new SparseDoubleLongKeyVector(other);
      streamVector = this;
    } else {
      baseVector = new SparseDoubleLongKeyVector(this);
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
    if (other instanceof SparseDoubleLongKeyVector)
      return plus((SparseDoubleLongKeyVector) other, x);

    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plus " + other.getClass()
        .getName());
  }

  private SparseDoubleLongKeyVector plus(SparseDoubleLongKeyVector other, double x) {
    assert dim == other.dim;
    SparseDoubleLongKeyVector baseVector = null;
    SparseDoubleLongKeyVector streamVector = null;
    if (size() < other.size()) {
      baseVector = new SparseDoubleLongKeyVector(other);
      streamVector = this;
    } else {
      baseVector = new SparseDoubleLongKeyVector(this);
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
    if (other instanceof SparseDoubleLongKeyVector)
      return dot((SparseDoubleLongKeyVector) other);

    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " dot " + other.getClass()
        .getName());
  }

  private double dot(SparseDoubleLongKeyVector other) {
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

  @Override public long[] getIndexes() {
    return indexToValueMap.keySet().toLongArray();
  }

  @Override public double[] getValues() {
    return indexToValueMap.values().toDoubleArray();
  }

  @Override public TVector set(long key, double value) {
    indexToValueMap.put(key, value);
    return this;
  }

  @Override public TVector times(double x) {
    SparseDoubleLongKeyVector vector = new SparseDoubleLongKeyVector(dim, indexToValueMap.size());
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
    SparseDoubleLongKeyVector vector = new SparseDoubleLongKeyVector(this.dim);

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
    return new SparseDoubleLongKeyVector(this);
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
    assert vector instanceof SparseDoubleLongKeyVector;

    this.matrixId = ((SparseDoubleLongKeyVector)vector).matrixId;
    this.rowId = ((SparseDoubleLongKeyVector)vector).rowId;
    this.clock = ((SparseDoubleLongKeyVector)vector).clock;
    this.indexToValueMap.clear();
    this.indexToValueMap.putAll(((SparseDoubleLongKeyVector)vector).indexToValueMap);
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

  @Override public MLProtos.RowType getType() {
    return MLProtos.RowType.T_DOUBLE_SPARSE_LONGKEY;
  }

  @Override public int size() {
    return indexToValueMap.size();
  }

  public double sum() {
    ObjectIterator<Long2DoubleMap.Entry> iter = indexToValueMap.long2DoubleEntrySet().iterator();
    double sum = 0;
    while (iter.hasNext()) {
      double v = iter.next().getDoubleValue();
      sum += v;
    }
    return sum;
  }

  public Long2DoubleOpenHashMap getIndexToValueMap() {
    return indexToValueMap;
  }
}
