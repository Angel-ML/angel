package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.protobuf.generated.MLProtos;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;

/**
 * Sparse Double Vector with long key which using one array as its backend storage. The vector indexes are sorted in ascending order.
 */
public class SparseDoubleLongKeySortedVector extends DoubleLongKeyVector{
  private final static Log LOG = LogFactory.getLog(SparseDoubleLongKeySortedVector.class);

  /**
   * Sorted index for non-zero items
   */
  long[] indices;

  /**
   * Number of non-zero items in this vector
   */
  int nnz;

  /**
   * Non-zero element values
   */
  public double[] values;


  /**
   * Init the vector with the vector dimension and index array capacity
   *
   * @param dim      vector dimension
   * @param capacity index array capacity
   */
  public SparseDoubleLongKeySortedVector(int capacity, long dim) {
    super(dim);
    this.nnz = 0;
    this.indices = new long[capacity];
    this.values = new double[capacity];
  }

  /**
   * Init the vector with the vector dimension, sorted non-zero indexes and values
   *
   * @param dim     vector dimension
   * @param indices sorted non-zero indexes
   * @param values  non-zero values
   */
  public SparseDoubleLongKeySortedVector(long dim, long[] indices, double[] values) {
    super(dim);
    this.nnz = indices.length;
    this.indices = indices;
    this.values = values;
  }

  /**
   * Init the vector by another vector
   *
   * @param other a SparseDoubleLongKeySortedVector with same dimension with this vector
   */
  public SparseDoubleLongKeySortedVector(SparseDoubleLongKeySortedVector other) {
    super(other.getLongDim());
    this.nnz = other.nnz;
    this.indices = new long[nnz];
    this.values = new double[nnz];
    System.arraycopy(other.indices, 0, this.indices, 0, this.nnz);
    System.arraycopy(other.values, 0, this.values, 0, nnz);
  }

  @Override
  public DoubleLongKeyVector plusBy(long index, double delta) {
    set(index, get(index) + delta);
    return this;
  }

  @Override public DoubleLongKeyVector set(long index, double value) {
    this.indices[nnz] = index;
    this.values[nnz] = value;
    nnz++;
    return this;
  }

  @Override public double get(long index) {
    int position = Arrays.binarySearch(indices, 0, nnz, index);
    if (position >= 0) {
      return values[position];
    }

    return 0.0;
  }

  @Override public long[] getIndexes() {
    return indices;
  }

  @Override
  public double sum() {
    double ret = 0.0;
    for(int i = 0; i < values.length; i++) {
      ret += values[i];
    }
    return ret;
  }

  @Override
  public void clone(TVector row) {
    if(row instanceof SparseDoubleLongKeySortedVector) {
      SparseDoubleLongKeySortedVector sortedRow = (SparseDoubleLongKeySortedVector) row;
      if (nnz == sortedRow.nnz) {
        System.arraycopy(sortedRow.indices, 0, this.indices, 0, this.nnz);
        System.arraycopy(sortedRow.values, 0, this.values, 0, nnz);
      } else {
        this.nnz = sortedRow.nnz;
        this.indices = new long[nnz];
        this.values = new double[nnz];
        System.arraycopy(sortedRow.indices, 0, this.indices, 0, this.nnz);
        System.arraycopy(sortedRow.values, 0, this.values, 0, nnz);
      }
    }

    throw new UnsupportedOperationException("Unsupport operation: clone " + row.getClass().getName() + " to " + this.getClass().getName());
  }

  @Override public TVector clone() {
    return new SparseDoubleLongKeySortedVector(this);
  }

  @Override
  public void clear() {
    this.nnz = 0;
    if (this.indices != null)
      this.indices = null;
    if (this.values != null)
      this.values = null;
  }

  @Override
  public double dot(TAbstractVector other) {
    if (other instanceof SparseDoubleLongKeyVector)
      return dot((SparseDoubleLongKeyVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " dot " + other.getClass().getName());
  }

  private double dot(SparseDoubleLongKeyVector other) {
    double ret = 0.0;
    long[] indexs = this.indices;
    double[] values = this.values;
    for (int i = 0; i < this.nnz; i++) {
      ret += values[i] * other.get(indexs[i]);
    }
    return ret;
  }

  @Override
  public TDoubleVector filter(double x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public MLProtos.RowType getType() {
    return MLProtos.RowType.T_DOUBLE_SPARSE_LONGKEY;
  }

  @Override
  public double[] getValues() {
    return values;
  }

  @Override
  public long nonZeroNumber() {
    long ret = 0;
    if (values != null) {
      for (int i = 0; i < values.length; i++) {
        if (values[i] != 0) {
          ret++;
        }
      }
    }
    return ret;
  }

  @Override
  public TDoubleVector plus(TAbstractVector other) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public TDoubleVector plus(TAbstractVector other, double x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public TDoubleVector plusBy(TAbstractVector other) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public TDoubleVector plusBy(TAbstractVector other, double x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public int size() {
    return nnz;
  }

  @Override
  public double sparsity() {
    return ((double) nnz) / dim;
  }

  @Override
  public double squaredNorm() {
    if(values == null) {
      return 0.0;
    }

    double norm = 0.0;
    for (int i = 0; i < values.length; i++)
      norm += values[i] * values[i];
    return norm;
  }

  @Override
  public DoubleLongKeyVector times(double x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public DoubleLongKeyVector timesBy(double x) {
    for (int i = 0; i < nnz; i++)
      values[i] *= x;
    return this;
  }
}
