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
import com.tencent.angel.protobuf.generated.MLProtos;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class DenseDoubleVectorTest {

  @Test
  public void cloneTest() throws Exception {
    int dim = 1000;
    double[] values = new double[dim];
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < dim; i++)
      values[i] = random.nextDouble();

    DenseIntDoubleVector vec = new DenseIntDoubleVector(dim, values);
    DenseIntDoubleVector vec_1 = new DenseIntDoubleVector(dim);
    vec_1.clone(vec);

    assertEquals(vec.squaredNorm(), vec_1.squaredNorm(), 0.0);
    assertArrayEquals(vec_1.getValues(), vec.getValues(), 0.0);

    TIntDoubleVector vec_2 = vec.clone();

    assertEquals(vec.squaredNorm(), ((DenseIntDoubleVector) vec_2).squaredNorm());
    assertArrayEquals(vec_2.getValues(), vec.getValues(), 0.0);

  }

  @Test
  public void clear() throws Exception {
    int dim = 10000;
    Random random = new Random(System.currentTimeMillis());

    double[] value = new double[dim];
    for (int i = 0; i < dim; i++)
      value[i] = random.nextDouble();

    DenseIntDoubleVector vec = new DenseIntDoubleVector(dim, value);

    vec.clear();

    for (int i = 0; i < vec.size(); i++)
      assertEquals(0.0, vec.get(i));

    DenseIntDoubleVector vec_1 = new DenseIntDoubleVector(dim);
    int nnz = random.nextInt(dim);
    for (int i = 0; i < nnz; i++)
      vec_1.set(i, random.nextDouble());

    vec_1.clear();

    for (int i = 0; i < vec.size(); i++)
      assertEquals(0.0, vec.get(i));


  }

  @Test
  public void dotDenseDoubleVector() throws Exception {
    int dim = 1000;
    Random random = new Random(System.currentTimeMillis());

    double[] values = new double[dim];
    double[] values_1 = new double[dim];
    for (int i = 0; i < dim; i++) {
      values[i] = random.nextDouble();
      values_1[i] = random.nextDouble();
    }

    DenseIntDoubleVector vec = new DenseIntDoubleVector(dim, values);
    TIntDoubleVector vec_1 = new DenseIntDoubleVector(dim, values_1);

    double sum = 0.0;
    for (int i = 0; i < dim; i++) {
      sum += values[i] * values_1[i];
    }

    assertEquals(sum, vec.dot(vec_1));
    assertEquals(sum, vec_1.dot(vec));
  }

  // @Test
  // public void dotDenseFloatVector() throws Exception {
  // int dim = 1000;
  // Random random = new Random(System.currentTimeMillis());
  //
  // double[] values = new double[dim];
  // float[] values_1 = new float[dim];
  // for (int i = 0; i < dim; i++) {
  // values[i] = random.nextDouble();
  // values_1[i] = random.nextFloat();
  // }
  //
  // DenseDoubleVector vec = new DenseDoubleVector(dim, values);
  // TDoubleVector vec_1 = new DenseFloatVector(dim, values_1);
  //
  // double sum = 0.0;
  // for (int i = 0; i < dim; i++) {
  // sum += values[i] * values_1[i];
  // }
  //
  // assertEquals(sum, vec.dot(vec_1));
  //
  // }

  @Test
  public void dotSparseDoubleVector() throws Exception {
    int dim = 1000;
    Random random = new Random(System.currentTimeMillis());

    double[] values = new double[dim];
    for (int i = 0; i < dim; i++) {
      values[i] = random.nextDouble();
    }

    TIntDoubleVector vec = new DenseIntDoubleVector(dim);

    int nnz = random.nextInt(dim);
    int[] indices_1 = genIndexes(dim, nnz);

    double[] values_1 = new double[nnz];
    for (int i = 0; i < nnz; i++) {
      values_1[i] = random.nextDouble();
    }

    TIntDoubleVector vec_1 = new SparseIntDoubleVector(dim, indices_1, values_1);

    double sum = 0.0;
    for (int i = 0; i < nnz; i++) {
      int idx = vec_1.getIndices()[i];
      double v = vec_1.get(idx);
      sum += v * vec.get(idx);
    }

    assertEquals(sum, vec.dot(vec_1));

  }

  @Test
  public void dotSparseDoubleSortedVector() throws Exception {
    DenseIntDoubleVector dVec = new DenseIntDoubleVector(5);
    for (int i = 0; i < 5; i++)
      dVec.set(i, i);

    TIntDoubleVector sVec = new SparseIntDoubleSortedVector(5, new int[] {1, 2}, new double[] {2, 4});

    assertEquals(10.0, dVec.dot(sVec));


    int dim = 1000;
    Random random = new Random(System.currentTimeMillis());

    double[] values = new double[dim];
    for (int i = 0; i < dim; i++) {
      values[i] = random.nextDouble();
    }

    DenseIntDoubleVector vec = new DenseIntDoubleVector(dim, values);

    int nnz = random.nextInt(dim);
    int[] indices_1 = genIndexes(dim, nnz);

    double[] values_1 = new double[nnz];
    for (int i = 0; i < nnz; i++) {
      values_1[i] = random.nextDouble();
    }

    TIntDoubleVector vec_1 = new SparseIntDoubleSortedVector(dim, indices_1, values_1);

    double sum = 0.0;
    for (int i = 0; i < nnz; i++) {
      sum += vec.get(indices_1[i]) * values_1[i];
    }

    assertEquals(sum, vec.dot(vec_1));
  }

  @Test
  public void dotSparseDummyVector() throws Exception {
    int dim = 1000;
    Random random = new Random(System.currentTimeMillis());

    double[] values = new double[dim];
    for (int i = 0; i < dim; i++) {
      values[i] = random.nextDouble();
    }

    DenseIntDoubleVector vec = new DenseIntDoubleVector(dim, values);

    TAbstractVector vec_1 = new SparseDummyVector(dim);
    int nnz = random.nextInt(dim);
    int[] indexs = genIndexes(dim, nnz);
    for (int idx : indexs)
      ((SparseDummyVector) vec_1).set(idx, 1);

    double sum = 0.0;
    for (int idx : indexs)
      sum += vec.get(idx);

    assertEquals(sum, vec.dot(vec_1));
  }

  @Test
  public void filter() throws Exception {
    int dim = 100;
    double[] value = new double[dim];
    for (int i = 0; i < dim; i++)
      value[i] = 0.1 * i;

    DenseIntDoubleVector vec = new DenseIntDoubleVector(dim, value);
    vec.setMatrixId(1);
    vec.setRowId(1);

    TIntDoubleVector vec_1 = vec.filter(2.0);

    assertEquals(vec.getMatrixId(), vec_1.getMatrixId());
    assertEquals(vec.getRowId(), vec_1.getRowId());
    assertArrayEquals(vec.getValues(), vec_1.getValues(), 0.0);

    TIntDoubleVector vec_2 = vec.filter(8.0);

    assertEquals(1, vec_2.getMatrixId());
    assertEquals(1, vec_2.getMatrixId());
    assertEquals(19, vec_2.nonZeroNumber());
    for (Double v : vec_2.getValues()) {
      assertTrue(v > 8);
    }
  }

  @Test
  public void get() throws Exception {
    DenseIntDoubleVector vec = new DenseIntDoubleVector(100);
    for (int i = 0; i < 100; i++)
      vec.set(i, i);

    for (int i = 0; i < 100; i++)
      assertEquals((double) i, vec.get(i));
  }

  @Test
  public void getValues() throws Exception {
    DenseIntDoubleVector vec = new DenseIntDoubleVector(5, new double[] {1.0, 0.0, 3.0, 0.0, 5.0});

    double[] values = vec.getValues();

    assertEquals(1.0, values[0]);
    assertEquals(0.0, values[1]);
    assertEquals(3.0, values[2]);
    assertEquals(0.0, values[3]);
    assertEquals(5.0, values[4]);
  }

  @Test
  public void getType() throws Exception {
    DenseIntDoubleVector vec = new DenseIntDoubleVector(10);
    assertEquals(MLProtos.RowType.T_DOUBLE_DENSE, vec.getType());
  }

  @Test
  public void nonZeroNumber() throws Exception {
    DenseIntDoubleVector vec = new DenseIntDoubleVector(5, new double[] {1.0, 0.0, 3.0, 0.0, 5.0});

    assertEquals(3, vec.nonZeroNumber());
  }

  @Test
  public void plusDenseDoubleVector() throws Exception {
    int dim = 1000;
    double[] values_1 = new double[dim];
    double[] values_2 = new double[dim];
    for (int i = 0; i < dim; i++) {
      values_1[i] = i;
      values_2[i] = 0.1 * i;
    }

    DenseIntDoubleVector vec_1 = new DenseIntDoubleVector(dim, values_1);
    TIntDoubleVector vec_2 = new DenseIntDoubleVector(dim, values_2);

    TIntDoubleVector vec_3 = vec_1.plus(vec_2);
    assertEquals(dim, vec_3.size());
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + vec_2.get(i), vec_3.get(i));

    TIntDoubleVector vec_4 = vec_1.plus(vec_2, 2);
    assertEquals(dim, vec_4.size());
    for (int i = 0; i < dim; i++)
      assertEquals(vec_4.get(i), vec_1.get(i) + vec_2.get(i) * 2);

    TIntDoubleVector vec_5 = vec_1.plus(vec_2, 2.0);
    assertEquals(dim, vec_5.size());
    for (int i = 0; i < dim; i++)
      assertEquals(vec_5.get(i), vec_1.get(i) + vec_2.get(i) * 2);

    double[] oldValues = vec_1.getValues().clone();

    vec_1.plusBy(vec_2);

    for (int i = 0; i < dim; i++)
      assertEquals(oldValues[i] + values_2[i], vec_1.get(i));

    oldValues = vec_1.getValues().clone();

    vec_1.plusBy(vec_2, 2);

    for (int i = 0; i < dim; i++)
      assertEquals(oldValues[i] + 2 * values_2[i], vec_1.get(i));
  }
  // @Test
  // public void plusDenseFlaotVector() throws Exception {
  // double[] value_1 = new double[]{0.1, 0.2, 0.3, 0.4, 0.5};
  // double[] value_2 = new double[]{0.1f, 0.2f, 0.3f, 0.4f, 0.5f};
  // DenseDoubleVector vec = new DenseDoubleVector(5, value_1);
  // TDoubleVector vec_1 = new DenseFloatVector(5, value_2);
  //
  // TDoubleVector vec_2 = vec.plus(vec_1);
  // for (int i = 0; i < vec.size(); i++)
  // assertEquals(value_1[i] + value_2[i], vec_2.get(i));
  //
  //
  // TDoubleVector vec_3 = vec.plus(vec_1, 2.0);
  //
  // for (int i = 0; i < vec.size(); i++)
  // assertEquals(vec_3.get(i), value_1[i] + 2 * value_2[i]);
  //
  // double[] oldValues = vec.getValues().clone();
  //
  // vec.plusBy(vec_1);
  //
  // for (int i = 0; i < vec.size(); i++)
  // assertEquals(vec.get(i), oldValues[i] + vec_1.get(i));
  //
  // oldValues = vec.getValues().clone();
  //
  // vec.plusBy(vec_1, 3);
  //
  // for (int i = 0; i < vec.size(); i++)
  // assertEquals(vec.get(i), oldValues[i] + 3 * vec_1.get(i));
  // }

  @Test
  public void plusSparDoubleVector() throws Exception {
    DenseIntDoubleVector vec = new DenseIntDoubleVector(5, new double[] {1, 2, 3, 4, 5});
    TIntDoubleVector vec_1 = new SparseIntDoubleVector(5, new int[] {2, 4}, new double[] {1.0, 2.0});

    TIntDoubleVector vec_2 = vec.plus(vec_1);

    assertEquals(1.0, vec_2.get(0));
    assertEquals(2.0, vec_2.get(1));
    assertEquals(4.0, vec_2.get(2));
    assertEquals(4.0, vec_2.get(3));
    assertEquals(7.0, vec_2.get(4));

    TIntDoubleVector vec_3 = vec.plus(vec_1, 0.5);

    assertEquals(1.0, vec_3.get(0));
    assertEquals(2.0, vec_3.get(1));
    assertEquals(3.5, vec_3.get(2));
    assertEquals(4.0, vec_3.get(3));
    assertEquals(6.0, vec_3.get(4));

    TIntDoubleVector vec_4 = vec.plus(vec_1, 3);

    assertEquals(1.0, vec_4.get(0));
    assertEquals(2.0, vec_4.get(1));
    assertEquals(6.0, vec_4.get(2));
    assertEquals(4.0, vec_4.get(3));
    assertEquals(11.0, vec_4.get(4));

    vec.plusBy(vec_1);

    assertEquals(1.0, vec.get(0));
    assertEquals(2.0, vec.get(1));
    assertEquals(4.0, vec.get(2));
    assertEquals(4.0, vec.get(3));
    assertEquals(7.0, vec.get(4));

    vec.plusBy(vec_1, 3);

    assertEquals(1.0, vec.get(0));
    assertEquals(2.0, vec.get(1));
    assertEquals(7.0, vec.get(2));
    assertEquals(4.0, vec.get(3));
    assertEquals(13.0, vec.get(4));
  }

  @Test
  public void plusSparseDoubleSortedVector() throws Exception {
    DenseIntDoubleVector vec = new DenseIntDoubleVector(5, new double[] {1, 2, 3, 4, 5});
    TIntDoubleVector vec_1 = new SparseIntDoubleSortedVector(5, new int[] {0, 1, 2, 3, 4},
        new double[] {0.0, 0.0, 1.0, 0.0, 2.0});

    TIntDoubleVector vec_2 = vec.plus(vec_1);

    assertEquals(1.0, vec_2.get(0));
    assertEquals(2.0, vec_2.get(1));
    assertEquals(4.0, vec_2.get(2));
    assertEquals(4.0, vec_2.get(3));
    assertEquals(7.0, vec_2.get(4));

    TIntDoubleVector vec_3 = vec.plus(vec_1, 0.5);

    assertEquals(1.0, vec_3.get(0));
    assertEquals(2.0, vec_3.get(1));
    assertEquals(3.5, vec_3.get(2));
    assertEquals(4.0, vec_3.get(3));
    assertEquals(6.0, vec_3.get(4));

    vec.plusBy(vec_1);

    assertEquals(1.0, vec.get(0));
    assertEquals(2.0, vec.get(1));
    assertEquals(4.0, vec.get(2));
    assertEquals(4.0, vec.get(3));
    assertEquals(7.0, vec.get(4));

    vec.plusBy(vec_1, 3);

    assertEquals(1.0, vec.get(0));
    assertEquals(2.0, vec.get(1));
    assertEquals(7.0, vec.get(2));
    assertEquals(4.0, vec.get(3));
    assertEquals(13.0, vec.get(4));
  }

  @Test
  public void plusSparseDummyVector() throws Exception {
    DenseIntDoubleVector vec = new DenseIntDoubleVector(5, new double[] {1, 2, 3, 4, 5});
    TAbstractVector vec_1 = new SparseDummyVector(5);
    ((SparseDummyVector) vec_1).set(2, 1);
    ((SparseDummyVector) vec_1).set(4, 1);

    vec.plusBy(vec_1);

    assertEquals(1.0, vec.get(0));
    assertEquals(2.0, vec.get(1));
    assertEquals(4.0, vec.get(2));
    assertEquals(4.0, vec.get(3));
    assertEquals(6.0, vec.get(4));

    vec.plusBy(vec_1, 5);

    assertEquals(1.0, vec.get(0));
    assertEquals(2.0, vec.get(1));
    assertEquals(9.0, vec.get(2));
    assertEquals(4.0, vec.get(3));
    assertEquals(11.0, vec.get(4));
  }

  @Test
  public void size() throws Exception {
    DenseIntDoubleVector vec = new DenseIntDoubleVector(5, new double[] {1.0, 0.0, 3.0, 0.0, 5.0});

    assertEquals(5, vec.size());
  }

  @Test
  public void sparsity() throws Exception {
    DenseIntDoubleVector vec = new DenseIntDoubleVector(5, new double[] {1.0, 0.0, 3.0, 0.0, 5.0});
    assertEquals(0.6, vec.sparsity(), 0.0);
  }

  @Test
  public void squaredNorm() throws Exception {
    DenseIntDoubleVector vec = new DenseIntDoubleVector(5, new double[] {1.0, 0.0, 3.0, 0.0, 5.0});

    assertEquals(35, vec.squaredNorm(), 0.0);
  }


  @Test
  public void times() throws Exception {
    DenseIntDoubleVector vec = new DenseIntDoubleVector(5, new double[] {1.0, 0.0, 3.0, 0.0, 5.0});
    TIntDoubleVector vec_1 = vec.times(2.0);
    assertEquals(2.0, vec_1.get(0));
    assertEquals(0.0, vec_1.get(1));
    assertEquals(6.0, vec_1.get(2));
    assertEquals(0.0, vec_1.get(3));
    assertEquals(10.0, vec_1.get(4));
  }

  @Test
  public void timesBy() throws Exception {
    DenseIntDoubleVector vec = new DenseIntDoubleVector(5, new double[] {1.0, 0.0, 3.0, 0.0, 5.0});

    vec.timesBy(2.0);

    assertEquals(2.0, vec.get(0));
    assertEquals(0.0, vec.get(1));
    assertEquals(6.0, vec.get(2));
    assertEquals(0.0, vec.get(3));
    assertEquals(10.0, vec.get(4));
  }

  public int[] genIndexes(int dim, int nnz) {
    ArrayList<Integer> indexList = new ArrayList<>();
    int[] indexs = new int[nnz];

    for (int i = 0; i < dim; i++)
      indexList.add(i);

    Random random = new Random(System.currentTimeMillis());

    for (int i = 0; i < nnz; i++) {
      int idx = indexList.get(random.nextInt(indexList.size()));
      indexs[i] = idx;
      indexList.remove(indexList.indexOf(idx));
    }

    return indexs;
  }

}
