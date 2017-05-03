/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.vector.TFloatVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.VectorType;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DenseFloatVector extends TFloatVector {

  private final static Log LOG = LogFactory.getLog(DenseFloatVector.class);

  /**
   * the value of vector
   */
  float[] values;

  /**
   * init the empty vector
   */
  public DenseFloatVector() {
    super();
  }

  /**
   * init the vector by another vector
   *
   * @param other
   */
  public DenseFloatVector(DenseFloatVector other) {
    super(other);
    this.dim = other.dim;
    this.values = new float[this.dim];
    System.arraycopy(other.values, 0, this.values, 0, dim);
  }

  /**
   * init the vector by setting the dim
   *
   * @param dim
   */
  public DenseFloatVector(int dim) {
    super();
    this.values = new float[dim];
    this.dim = dim;
  }

  /**
   * init the vector by setting the dim and values
   *
   * @param dim
   * @param values
   */
  public DenseFloatVector(int dim, float[] values) {
    super();
    this.dim = dim;

    if (this.values == null) {
      this.values = new float[dim];
    }

    this.values = values;
  }

  /**
   * clear the vector
   */
  @Override
  public void clear() {
    if (values != null) {
      for (int i = 0; i < values.length; i++) {
        values[i] = 0;
      }
    }
  }

  /**
   * clone the vector
   *
   * @return
   */
  @Override
  public TFloatVector clone() {
    return new DenseFloatVector(this);
  }

  /**
   * clone vector by another one
   *
   * @return
   */
  @Override
  public void clone(TVector row) {
    if (row instanceof DenseFloatVector) {
      this.rowId = ((DenseFloatVector) row).rowId;
      this.matrixId = ((DenseFloatVector) row).matrixId;
      this.dim = ((DenseFloatVector) row).dim;
      this.clock = ((DenseFloatVector) row).clock;;

      if (this.values == null)
        this.values = new float[dim];

      System.arraycopy(((DenseFloatVector) row).values, 0, this.values, 0, dim);
    } else {
      LOG.error("unregister type for DenseFloatVector clone.");
    }
  }


  /**
   * calculate the inner product
   *
   * @param other
   * @return
   */
  public double dot(DenseFloatVector other) {
    double ret = 0.0;
    for (int i = 0; i < dim; i++) {
      ret += this.values[i] * other.values[i];
    }
    return ret;
  }

  public double dot(DenseDoubleVector other) {
    double ret = 0.0;
    for (int i = 0; i < dim; i++) {
      ret += this.values[i] * (float) other.values[i];
    }
    return ret;
  }

  @Override
  public double dot(TAbstractVector other) {
    if (other instanceof DenseFloatVector)
      return dot((DenseFloatVector) other);
    if (other instanceof DenseDoubleVector)
      return dot((DenseDoubleVector) other);

    LOG.error(String.format("unregisterd vector type %s", other.getClass().getName()));
    return 0.0;
  }

  /**
   * filter the vector and covert to the appropriate type
   *
   * @param x the comparison value
   * @return
   */
  @Override
  public TVector filter(double x) {
    IntArrayList nonzeroIndex = new IntArrayList();
    int nonzero = 0;
    for (int i = 0; i < values.length; i++) {
      if (Math.abs(values[i]) > (float) x) {
        nonzero++;
        nonzeroIndex.add(i);
      }
    }

    if (nonzero < values.length * 0.5) {
      LOG.debug(String.format("Dense Row filter generate a sparse row with nonzero %d", nonzero));
      int[] newIndex = new int[nonzero];
      System.arraycopy(nonzeroIndex.elements(), 0, newIndex, 0, nonzero);
      float[] newValue = new float[nonzero];
      for (int i = 0; i < nonzero; i++) {
        newValue[i] = values[newIndex[i]];
      }

      SparseFloatVector ret = new SparseFloatVector(dim, newIndex, newValue);
      ret.setRowId(rowId).setMatrixId(matrixId).setClock(clock);
      return ret;
    } else {
      return this;
    }
  }

  /**
   * get the element by index
   *
   * @param index the index
   * @return
   */
  @Override
  public float get(int index) {
    return values[index];
  }

  /**
   * get values of all of the elements
   *
   * @return
   */
  @Override
  public float[] getValues() {
    float[] ret = new float[this.size()];
    for (int i = 0; i < this.size(); i++) {
      ret[i] = values[i];
    }
    return ret;
  }

  /**
   * get the type
   *
   * @return
   */
  @Override
  public VectorType getType() {
    return VectorType.T_FLOAT_DENSE;
  }

  /**
   * get all of the index
   *
   * @return
   */
  @Override
  public int[] getIndices() {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * count the nonzero element
   *
   * @return
   */
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
  public TFloatVector plus(TAbstractVector other) {
    if (other instanceof DenseDoubleVector)
      return plus((DenseDoubleVector) other);
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other);
    LOG.error(String.format("unregisterd vector type %s", other.getClass().getName()));
    return null;
  }

  public TFloatVector plus(DenseDoubleVector other) {
    assert dim == other.size();
    DenseFloatVector vector = new DenseFloatVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + (float) other.values[i];
    return vector;
  }

  public TFloatVector plus(DenseFloatVector other) {
    assert dim == other.size();
    DenseFloatVector vector = new DenseFloatVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i];
    return vector;
  }

  /**
   * plus the vector by another vector and element
   *
   * @param other the other
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TFloatVector plus(TAbstractVector other, double x) {
    if (other instanceof DenseDoubleVector)
      return plus((DenseDoubleVector) other, x);
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other, x);

    LOG.error(String.format("unregisterd vector type %s", other.getClass().getName()));
    return null;
  }

  public TFloatVector plus(TAbstractVector other, float x) {
    return plus(other, (double) x);
  }

  @Override
  public TFloatVector plus(TAbstractVector other, int x) {
    return plus(other, (double) x);
  }

  public TFloatVector plus(DenseDoubleVector other, double x) {
    assert dim == other.size();
    DenseFloatVector vector = new DenseFloatVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + (float) (other.values[i] * x);
    return vector;
  }

  public TFloatVector plus(DenseFloatVector other, double x) {
    assert dim == other.size();
    DenseFloatVector vector = new DenseFloatVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i] * (float) x;
    return vector;
  }

  @Override
  public TVector plusBy(TAbstractVector other) {
    if (other instanceof DenseDoubleVector)
      return plusBy((DenseDoubleVector) other);
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other);

    LOG.error(String.format("Unregistered vector type %s", other.getClass().getName()));
    return null;
  }

  private TFloatVector plusBy(DenseDoubleVector other) {
    double[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += (float) delta[i];
    }
    return this;
  }

  private TFloatVector plusBy(DenseFloatVector other) {
    float[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += delta[i];
    }
    return this;
  }

  public TFloatVector plusBy(SparseFloatVector other) {
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2FloatMap.Entry entry = iter.next();
      values[entry.getIntKey()] += entry.getFloatValue();
    }

    return this;
  }

  @Override
  public TVector plusBy(TAbstractVector other, double x) {
    if (other instanceof DenseDoubleVector)
      return plusBy((DenseDoubleVector) other, x);
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other, x);
    if (other instanceof SparseDoubleVector)
      return plusBy((SparseDoubleVector) other, x);
    if (other instanceof SparseDoubleSortedVector)
      return plusBy((SparseDoubleSortedVector) other, x);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other, x);

    LOG.error(String.format("Unregistered vector type %s", other.getClass().getName()));
    return null;
  }

  @Override
  public TVector plusBy(TAbstractVector other, int x) {
    return plusBy(other, (double) x);
  }

  private TFloatVector plusBy(DenseDoubleVector other, double x) {
    double[] delts = other.getValues();
    for (int i = 0; i < delts.length; i++) {
      this.values[i] += (float) (delts[i] * x);
    }
    return this;
  }

  private TFloatVector plusBy(DenseFloatVector other, double x) {
    float fx = (float) x;
    float[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += delta[i] * fx;
    }
    return this;
  }

  private TFloatVector plusBy(SparseDoubleVector other, double x) {
    float fx = (float) x;
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2DoubleMap.Entry entry = iter.next();
      values[entry.getIntKey()] += (float) entry.getDoubleValue() * fx;
    }

    return this;
  }

  private TFloatVector plusBy(SparseDoubleSortedVector other, double x) {
    float inner_product = 0.0f;
    float fx = (float) x;
    int[] keys = other.getIndices();
    double[] vals = other.getValues();
    for (int i = 0; i < keys.length; i++) {
      inner_product += values[keys[i]] * (float) vals[i] * fx;
      values[keys[i]] += (float) vals[i] * fx;
    }

    return this;
  }

  private TFloatVector plusBy(SparseFloatVector other, double x) {
    float fx = (float) x;
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2FloatMap.Entry entry = iter.next();
      values[entry.getIntKey()] += fx * entry.getFloatValue();
    }

    return this;
  }

  private TFloatVector plusBy(int[] indexes, double[] deltas, double x) {
    float fx = (float) x;
    int length = indexes.length;
    for (int i = 0; i < length; i++) {
      values[indexes[i]] += deltas[i] * fx;
    }
    return this;
  }

  private TFloatVector plusBy(int[] indexes, float[] deltas, double x) {
    float fx = (float) x;
    int length = indexes.length;
    for (int i = 0; i < length; i++) {
      values[indexes[i]] += deltas[i] * fx;
    }
    return this;
  }

  private TFloatVector plusBy(int[] indexes, double[] deltas) {
    int length = indexes.length;
    for (int i = 0; i < length; i++) {
      values[indexes[i]] += (float) deltas[i];
    }
    return this;
  }

  private TFloatVector plusBy(int[] indexes, float[] deltas) {
    int length = indexes.length;
    for (int i = 0; i < length; i++) {
      values[indexes[i]] += deltas[i];
    }
    return this;
  }

  /**
   * set the value by index
   *
   * @param index the index
   * @param value the value
   */
  @Override
  public void set(int index, double value) {
    values[index] = (float) value;
  }

  public void set(int index, float value) {
    values[index] = value;
  }


  /**
   * get the size
   *
   * @return
   */
  @Override
  public int size() {
    return values.length;
  }

  /**
   * get the sparsity
   *
   * @return
   */
  @Override
  public double sparsity() {
    int nonzero = 0;
    for (int i = 0; i < values.length; i++) {
      if (Math.abs(values[i]) > 0) {
        nonzero += 1;
      }
    }
    return ((double) nonzero) / values.length;
  }

  /**
   * get the norm of vector
   *
   * @return
   */
  @Override
  public double squaredNorm() {
    double squre = 0.0;
    for (int i = 0; i < dim; i++)
      squre += values[i] * values[i];
    return squre;
  }

  /**
   * the multiplication of vector and element and do not change the vector
   *
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TFloatVector times(double x) {
    DenseFloatVector vector = new DenseFloatVector(this.dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] * (float) x;
    return vector;
  }

  /**
   * the multiplication of vector and element and change the vector
   *
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TFloatVector timesBy(double x) {
    float fx = (float) x;
    for (int i = 0; i < dim; i++)
      values[i] *= fx;

    return this;
  }

}
