package com.tencent.angel.ml.math.vector;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.TVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Base class for long key double vector.
 */
public abstract class DoubleLongKeyVector extends TVector {
  private static final Log LOG = LogFactory.getLog(SparseDoubleLongKeyVector.class);

  /** Vector dimension */
  protected long dim;
  public DoubleLongKeyVector(long dim) {
    super();
    this.dim = dim;
  }

  /**
   * Get dimension for long key vector
   * @return long dimension
   */
  public long getLongDim(){
    return dim;
  }

  /**
   * Plus a element by a update value
   * @param index element index
   * @param x update value
   * @return this
   */
  public abstract TVector plusBy(long index, double x);

  /**
   * Set a element by a new value
   * @param index element index
   * @param x new value
   * @return this
   */
  public abstract TVector set(long index, double x);

  /**
   * Get a element value by value index
   * @param index value index
   * @return
   */
  public abstract double get(long index);

  public abstract long[] getIndexes();

  public abstract double[] getValues();

  @Override
  public int getDimension() {
    throw new UnsupportedOperationException("Unsupportted operation, you should use getLongDim instead");
  }

  @Override
  public TVector plusBy(int index, double x) {
    return plusBy((long) index, x);
  }

  public abstract double sum();
}
