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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.ml.math.VectorType;
import org.junit.Test;

import java.util.Random;

import static com.tencent.angel.ml.Utils.*;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class SparseDoubleSortedVectorTest {
  @Test
  public void cloneTest() throws Exception {
    SparseDoubleSortedVector vec = genSparseDoubleSortedVector(300, 10000);

    SparseDoubleSortedVector vec_1 = vec.clone();

    assertEquals(vec.squaredNorm(), vec_1.squaredNorm());
    assertArrayEquals(vec.getIndices(), vec_1.getIndices());
    assertArrayEquals(vec.getValues(), vec_1.getValues(), 0.0);

    SparseDoubleSortedVector vec_2 = new SparseDoubleSortedVector(300, 10000);

    vec_2.clone(vec);

    assertEquals(vec.squaredNorm(), vec_2.squaredNorm());
    assertArrayEquals(vec.getIndices(), vec_2.getIndices());
    assertArrayEquals(vec.getValues(), vec_2.getValues(), 0.0);

    SparseDoubleSortedVector vec_3 = genSparseDoubleSortedVector(300, 10000);

    vec_3.clone(vec);

    assertEquals(vec.squaredNorm(), vec_3.squaredNorm());
    assertArrayEquals(vec.getIndices(), vec_3.getIndices());
    assertArrayEquals(vec.getValues(), vec_3.getValues(), 0.0);

  }

  @Test
  public void clear() throws Exception {
    SparseDoubleSortedVector vec = genSparseDoubleSortedVector(300, 10000);

    vec.clear();

    assertEquals(0.0, vec.squaredNorm());
    assertEquals(0, vec.nonZeroNumber());
    assertEquals(null, vec.getIndices());
    assertEquals(null, vec.getValues());
  }

  @Test
  public void dot() throws Exception {
    SparseDoubleSortedVector vec = genSparseDoubleSortedVector(300, 10000);
    TDoubleVector vec_1 = genDenseDoubleVector(10000);

    int[] indexs = vec.getIndices();
    double[] values_1 = vec.getValues();
    double[] values_2 = vec_1.getValues();

    double sum = 0.0;
    for (int i = 0; i < indexs.length; i++)
      sum += values_1[i] * values_2[indexs[i]];

    assertEquals(sum, vec.dot(vec_1));
  }

  @Test
  public void get() throws Exception {
    int[] index = genSortedIndexs(300, 10000);
    double[] values = genDoubleArray(300);

    SparseDoubleSortedVector vec = new SparseDoubleSortedVector(10000, index, values);

    for (int i = 0; i < 300; i++) {
      int idx = index[i];
      assertEquals(values[i], vec.get(idx));
    }
  }

  @Test
  public void getDimensionAndNNZ() throws Exception {
    int dim = 10000;
    Random random = new Random(System.currentTimeMillis());

    for (int i = 0; i < 100; i++) {
      int nnz = random.nextInt(dim);

      SparseDoubleSortedVector vec = genSparseDoubleSortedVector(nnz, dim);

      assertEquals(dim, vec.getDimension());
      assertEquals(nnz, vec.nonZeroNumber());
    }
   }

  @Test
  public void getIndices() throws Exception {
    int[] indexs = genSortedIndexs(300, 10000);
    double[] values = genDoubleArray(300);

    SparseDoubleSortedVector vec = new SparseDoubleSortedVector(10000, indexs, values);

    assertArrayEquals(indexs, vec.getIndices());
  }

  @Test
  public void getType() throws Exception {
    SparseDoubleSortedVector vec = new SparseDoubleSortedVector(10000, 300);
    assertEquals(VectorType.T_DOUBLE_SPARSE, vec.getType());
  }

  @Test
  public void getValues() throws Exception {
    int[] indexs = genSortedIndexs(300, 10000);
    double[] values = genDoubleArray(300);

    SparseDoubleSortedVector vec = new SparseDoubleSortedVector(10000, indexs, values);

    assertArrayEquals(values, vec.getValues(), 0.0);
  }

  @Test
  public void getNorm() throws Exception {
    int[] indexs = genSortedIndexs(300, 10000);
    double[] values = genDoubleArray(300);

    double sum = 0;
    for (double v: values)
      sum += v * v;

    SparseDoubleSortedVector vec = new SparseDoubleSortedVector(10000, indexs, values);

    assertEquals(sum, vec.squaredNorm());
  }

  @Test
  public void nonZeroNumber() throws Exception {
    int[] indexs = genSortedIndexs(300, 10000);
    double[] values = genDoubleArray(300);

    int nnz = 0;
    for (double v: values) {
      if ( v != 0)
        nnz +=1;
    }

    SparseDoubleSortedVector vec = new SparseDoubleSortedVector(10000, indexs, values);

    assertEquals(nnz, vec.nonZeroNumber());
  }


  @Test
  public void set() throws Exception {
    SparseDoubleSortedVector vec = new SparseDoubleSortedVector(300, 10000);
    int[] indexs = genSortedIndexs(300, 10000);
    double[] value = genDoubleArray(300);

    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < 300; i++)
      vec.set(indexs[i], value[i]);

    for (int i = 0; i < 300; i++)
      assertEquals(value[i], vec.get(indexs[i]));
  }

  @Test
  public void size() throws Exception {
    SparseDoubleSortedVector vec = genSparseDoubleSortedVector(10000);
    int nnz = vec.getValues().length;

    assertEquals(nnz, vec.size());
  }

  @Test
  public void sparsity() throws Exception {
    SparseDoubleSortedVector vec = genSparseDoubleSortedVector(300, 10000);

    assertEquals(0.03, vec.sparsity());
  }

  @Test
  public void squaredNorm() throws Exception {
    SparseDoubleSortedVector vec = genSparseDoubleSortedVector(300, 10000);
    double[] value = vec.getValues();
    double sum = 0;
    for (double v : value)
      sum += v * v;

    assertEquals(sum, vec.squaredNorm());
  }

  @Test
  public void times() throws Exception {
    SparseDoubleSortedVector vec = genSparseDoubleSortedVector(300, 10000);
    SparseDoubleSortedVector vec_1 = (SparseDoubleSortedVector) vec.times(3.0);

    assertArrayEquals(vec.getIndices(), vec_1.getIndices());
    for (int i = 0; i < vec.getDimension(); i++)
      assertEquals(vec.get(i) * 3.0, vec_1.get(i));
  }

  @Test
  public void timesBy() throws Exception {
    SparseDoubleSortedVector vec = genSparseDoubleSortedVector(300,10000);
    double[] value = vec.getValues().clone();

    vec.timesBy(3.0);

    double[] value_1 = vec.getValues().clone();

    for (int i = 0; i < 300; i++)
      assertEquals(value[i] * 3.0, value_1[i]);
  }
}
