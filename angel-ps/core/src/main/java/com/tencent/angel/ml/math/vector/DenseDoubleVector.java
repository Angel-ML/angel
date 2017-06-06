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
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.VectorType;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map.Entry;

public class DenseDoubleVector extends TDoubleVector {

  private final static Log LOG = LogFactory.getLog(DenseDoubleVector.class);
  /**
   * the value of vector
   */
  double[] values;

  /**
   * the sum of element square
   */
  double norm;

  /**
   * init the empty vector
   */
  public DenseDoubleVector() {
    super();
    norm = 0;
  }

  /**
   * init the vector by another vector
   * 
   * @param other
   */
  public DenseDoubleVector(DenseDoubleVector other) {
    super(other);
    this.values = new double[this.dim];
    this.norm = other.norm;
    System.arraycopy(other.values, 0, this.values, 0, dim);
  }

  /**
   * init the vector by setting the dim
   * 
   * @param dim
   */
  public DenseDoubleVector(int dim) {
    super();
    this.values = new double[dim];
    this.dim = dim;
    this.norm = 0;
  }

  /**
   * init the vector by setting the dim and values
   * 
   * @param dim
   * @param values
   */
  public DenseDoubleVector(int dim, double[] values) {
    super();
    this.dim = dim;
    this.values = values;
    this.norm = 0;

    for (int i = 0; i < values.length; i++)
      norm += values[i] * values[i];
  }

  /**
   * clear the vector
   */
  @Override
  public void clear() {
    if (values != null) {
      for (int i = 0; i < values.length; i++) {
        values[i] = 0.0;
      }
    }
  }

  /**
   * clone the vector
   * 
   * @return
   */
  @Override
  public DenseDoubleVector clone() {
    return new DenseDoubleVector(this);
  }

  @Override
  public void clone(TVector row) {
    System.arraycopy(((DenseDoubleVector) row).values, 0, this.values, 0, dim);
    this.norm = ((DenseDoubleVector) row).norm;
    this.rowId = ((DenseDoubleVector) row).rowId;
  }

  /**
   * calculate the inner product
   * 
   * @param other
   * @return
   */
  public double dot(DenseDoubleVector other) {
    double ret = 0.0;
    for (int i = 0; i < dim; i++) {
      ret += this.values[i] * other.values[i];
    }
    return ret;
  }

  public double dot(DenseFloatVector other) {
    double ret = 0.0;
    for (int i = 0; i < dim; i++) {
      ret += this.values[i] * other.values[i];
    }
    return ret;
  }

  public double dot(SparseDummyVector other) {
    double _ret = 0.0;
    for (int i = 0; i < other.nonzero; i++) {
      _ret += this.values[other.indices[i]];
    }
    return _ret;
  }

  public double dot(SparseDoubleVector other) {
    return other.dot(this);
  }

  /**
   * Dot Functions
   */
  public double dot(SparseDoubleSortedVector other) {
    return other.dot(this);
  }

  @Override
  public double dot(TAbstractVector other) {
    if (other instanceof SparseDoubleSortedVector)
      return dot((SparseDoubleSortedVector) other);
    if (other instanceof SparseDoubleVector)
      return dot((SparseDoubleVector) other);
    if (other instanceof DenseDoubleVector)
      return dot((DenseDoubleVector) other);
    if (other instanceof SparseDummyVector)
      return dot((SparseDummyVector) other);
    if (other instanceof DenseFloatVector)
      return dot((DenseFloatVector) other);

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
  public TDoubleVector filter(double x) {
    IntArrayList nonzeroIndex = new IntArrayList();
    int nonzero = 0;
    for (int i = 0; i < values.length; i++) {
      if (Math.abs(values[i]) > x) {
        nonzero++;
        nonzeroIndex.add(i);
      }
    }

    if (nonzero < values.length * 0.5) {
      LOG.debug(String.format("Dense Row filter generate a sparse row with nonzero %d", nonzero));
      int[] newIndex = new int[nonzero];
      System.arraycopy(nonzeroIndex.elements(), 0, newIndex, 0, nonzero);
      double[] newValue = new double[nonzero];
      for (int i = 0; i < nonzero; i++) {
        newValue[i] = values[newIndex[i]];
      }

      SparseDoubleSortedVector ret = new SparseDoubleSortedVector(dim, newIndex, newValue);
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
  public double get(int index) {
    return values[index];
  }

  /**
   * get values of all of the elements
   * 
   * @return
   */
  @Override
  public double[] getValues() {
    return values;
  }

  /**
   * get the type
   * 
   * @return
   */
  @Override
  public VectorType getType() {
    return VectorType.T_DOUBLE_DENSE;
  }

  /**
   * get all of the index
   * 
   * @return
   */
  @Override
  public int[] getIndices() {
    int [] indices=new int[values.length];
    for(int i=0;i<indices.length;i++)
      indices[i]=i;
    return indices;
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

  /**
   * plus the vector by another vector
   * 
   * @param other the other
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TDoubleVector plus(TAbstractVector other, double x) {
    if (other instanceof DenseDoubleVector)
      return plus((DenseDoubleVector) other, x);
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other, x);
    if (other instanceof SparseDoubleVector)
      return plus((SparseDoubleVector) other, x);
    if (other instanceof SparseDoubleSortedVector)
      return plus((SparseDoubleSortedVector) other, x);
    return null;
  }


  @Override
  public TDoubleVector plus(TAbstractVector other, int x) {
    return plus(other, (double) x);
  }

  public TDoubleVector plus(DenseDoubleVector other, double x) {
    assert dim == other.dim;
    DenseDoubleVector vector = new DenseDoubleVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i] * x;
    return vector;
  }

  public TDoubleVector plus(DenseFloatVector other, double x) {
    assert dim == other.size();
    DenseDoubleVector vector = new DenseDoubleVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i] * x;
    return vector;
  }

  public TDoubleVector plus(SparseDoubleVector other, double x) {
    DenseDoubleVector vector = (DenseDoubleVector) this.clone();

    for (Entry<Integer, Double> entry : other.hashMap.entrySet()) {
      vector.values[entry.getKey().intValue()] += entry.getValue().doubleValue() * x;
    }
    return vector;
  }

  public TDoubleVector plus(SparseDoubleSortedVector other, double x) {
    assert dim == other.getDimension();
    DenseDoubleVector vector = (DenseDoubleVector) this.clone();
    int length = other.indices.length;
    for (int i = 0; i < length; i++) {
      vector.values[other.indices[i]] += other.values[i] * x;
    }
    return vector;
  }

  @Override
  public TDoubleVector plus(TAbstractVector other) {
    if (other instanceof DenseDoubleVector)
      return plus((DenseDoubleVector) other);
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other);
    if (other instanceof SparseDoubleVector)
      return plus((SparseDoubleVector) other);
    if (other instanceof SparseDoubleSortedVector)
      return plus((SparseDoubleSortedVector) other);

    LOG.error(
        String.format("Unregistered vector type %s for plus(other)", other.getClass().getName()));
    return null;
  }



  public TDoubleVector plus(DenseDoubleVector other) {
    assert dim == other.dim;
    DenseDoubleVector vector = new DenseDoubleVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i];
    return vector;
  }

  public TDoubleVector plus(DenseFloatVector other) {
    assert dim == other.size();
    DenseDoubleVector vector = new DenseDoubleVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + (double) other.values[i];
    return vector;
  }

  public TDoubleVector plus(SparseDoubleVector other) {
    DenseDoubleVector vector = (DenseDoubleVector) this.clone();
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getDoubleValue();
    }
    return vector;
  }

  public TDoubleVector plus(SparseDoubleSortedVector other) {
    assert dim == other.getDimension();
    DenseDoubleVector vector = (DenseDoubleVector) this.clone();
    int length = other.indices.length;
    for (int i = 0; i < length; i++) {
      vector.values[other.indices[i]] += other.values[i];
    }
    return vector;
  }

  public void inc(int index, double value) {
    values[index] += value;
  }

  @Override
  public TDoubleVector plusBy(TAbstractVector other, double x) {
    if (other instanceof DenseDoubleVector)
      return plusBy((DenseDoubleVector) other, x);
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other, x);
    if (other instanceof SparseDoubleVector)
      return plusBy((SparseDoubleVector) other, x);
    if (other instanceof SparseDoubleSortedVector)
      return plusBy((SparseDoubleSortedVector) other, x);
    if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other, x);

    LOG.error(String.format("Unregistered vector type %s", other.getClass().getName()));
    return null;
  }

  @Override
  public TDoubleVector plusBy(TAbstractVector other, int x) {
    return plusBy(other, (double) x);
  }

  private TDoubleVector plusBy(DenseDoubleVector other, double x) {
    double[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += delta[i] * x;
    }
    return this;
  }

  private TDoubleVector plusBy(DenseFloatVector other, double x) {
    float[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += delta[i] * x;
    }
    return this;
  }

  private TDoubleVector plusBy(SparseDoubleVector other, double x) {

    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2DoubleMap.Entry entry = iter.next();
      values[entry.getIntKey()] += entry.getDoubleValue() * x;
    }

    return this;
  }

  private TDoubleVector plusBy(SparseDoubleSortedVector other, double x) {
    double inner_product = 0.0;
    int[] keys = other.getIndices();
    double[] vals = other.getValues();
    for (int i = 0; i < keys.length; i++) {
      inner_product += values[keys[i]] * vals[i] * x;
      values[keys[i]] += vals[i] * x;
    }

    norm += other.squaredNorm() * x * x + (2.0 * inner_product);
    return this;
  }

  private TDoubleVector plusBy(SparseDummyVector other, double x) {
    for (int i = 0; i < other.nonzero; i++) {
      this.values[other.indices[i]] += x;
    }
    return this;
  }

  private TDoubleVector plusBy(DenseDoubleVector other) {
    double[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += delta[i];
    }
    return this;
  }

  private TDoubleVector plusBy(DenseFloatVector other) {
    float[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += delta[i];
    }
    return this;
  }

  @Override
  public TDoubleVector plusBy(TAbstractVector other) {
    if (other instanceof DenseDoubleVector)
      return plusBy((DenseDoubleVector) other);
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other);
    if (other instanceof SparseDoubleVector)
      return plusBy((SparseDoubleVector) other);
    if (other instanceof SparseDoubleSortedVector)
      return plusBy((SparseDoubleSortedVector) other);
    if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other);

    LOG.error(
        String.format("Unregistered vector type %s for plus(other)", other.getClass().getName()));
    return null;
  }

  private TDoubleVector plusBy(SparseDoubleVector other) {
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();

    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      values[entry.getIntKey()] += entry.getDoubleValue();
    }
    return this;
  }

  private TDoubleVector plusBy(SparseDoubleSortedVector other) {
    return plusBy(other.indices, other.getValues());
  }

  private TDoubleVector plusBy(SparseDummyVector other) {
    for (int i = 0; i < other.nonzero; i++) {
      this.values[other.indices[i]] += 1.0;
    }
    return this;
  }

  private TDoubleVector plusBy(int[] indexes, double[] deltas) {
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
    return norm;
  }

  /**
   * the multiplication of vector and element
   *
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TDoubleVector times(double x) {
    DenseDoubleVector vector = new DenseDoubleVector(this.dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] * x;
    return vector;
  }

  @Override
  public TDoubleVector timesBy(double x) {
    for (int i = 0; i < dim; i++)
      values[i] *= x;

    norm *= (x * x);
    return this;
  }

  /**
   * get the square of the vector
   * 
   * @return
   */
  public TDoubleVector getSquaredVector() {
    DenseDoubleVector vector = new DenseDoubleVector(this.dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] * values[i];

    vector.norm = 0;
    for (int i = 0; i < values.length; i++)
      vector.norm += vector.values[i] * vector.values[i];
    return vector;
  }

  /**
   * get the sum of vector
   * 
   * @return
   */
  public double getSum() {
    double sum = 0;
    for (int i = 0; i < dim; i++) {
      sum = sum + values[i];
    }
    return sum;
  }

  /**
   * get index of the max value
   * 
   * @return
   */
  public int getMaxValueIndex() {
    int index = 0;
    double max_value = values[0];
    for (int i = 1; i < dim; i++) {
      if (max_value < values[i]) {
        max_value = values[i];
        index = i;
      }
    }
    return index;
  }

}
