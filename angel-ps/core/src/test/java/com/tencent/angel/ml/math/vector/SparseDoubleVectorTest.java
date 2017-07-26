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

import com.tencent.angel.ml.math.VectorType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static com.tencent.angel.ml.Utils.*;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;


public class SparseDoubleVectorTest {
  private static int dim = 100;
  private static int cap = 20;
  private static int nnz = 30;

  @Test
  public void cloneTest() throws Exception {
    SparseDoubleVector vec = genSparseDoubleVector(300, 10000);

    SparseDoubleVector vec_1 = (SparseDoubleVector) vec.clone();

    int[] index = vec.getIndices();
    int[] index1 = vec.getIndices();
    Arrays.sort(index);
    Arrays.sort(index1);
    assertArrayEquals(index, index1);
    for (int i = 0; i < index.length; i++)
      assertEquals(vec.get(i), vec_1.get(i));

    TDoubleVector vec_2 = new SparseDoubleVector(10000, 500);
    vec_2.clone(vec);
    int[] index2 = vec_2.getIndices();
    Arrays.sort(index2);
    assertArrayEquals(index, index2);
    for (int i = 0; i < index.length; i++)
      assertEquals(vec.get(i), vec_2.get(i));
  }


  @Test
  public void clear() throws Exception {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);

    vec.clear();

    assertTrue(vec.getIndices().length == 0);
    assertTrue(vec.getValues().length == 0);
  }


  @Test
  public void denseDoubleComputeTest() throws Exception {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);

    TDoubleVector vec_1 = genDenseDoubleVector(dim);

    //dot
    double sum  =0.0;
    for (int i = 0; i < dim; i++)
      sum += vec_1.get(i) * vec.get(i);
    double dot = vec.dot(vec_1);
    assertEquals(sum, dot);

    //plus
    TDoubleVector vec_2 = vec.plus(vec_1);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + vec.get(i), vec_2.get(i));
    TDoubleVector vec_3 = vec.plus(vec_1, 3);
    for (int i = 0; i < dim; i++)
      assertEquals(3 * vec_1.get(i) + vec.get(i), vec_3.get(i));

    //plusBy
    TDoubleVector vec_5 = vec.clone();
    vec_5.plusBy(vec_1);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + vec_1.get(i), vec_5.get(i));
    TDoubleVector vec_6 = vec.clone();
    vec_6.plusBy(vec_1, 4);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + 4 * vec_1.get(i), vec_6.get(i));

  }

  @Test
  public void sparseDoubleComputeTest() {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);
    TDoubleVector vec_1 = genSparseDoubleVector(nnz, dim);

    //dot
    double sum = 0.0;
    for (int i = 0; i < dim; i++)
      sum += vec.get(i) * vec_1.get(i);
    double dot = vec.dot(vec_1);
    assertEquals(sum, dot, 0.00000000001);

    //plus
    TDoubleVector vec_2 = vec.plus(vec_1);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + vec_1.get(i), vec_2.get(i));
    TDoubleVector vec_3 = vec.plus(vec_1, 4);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + 4 * vec_1.get(i), vec_3.get(i));

    //plusBy
    TDoubleVector vec_4 = vec.clone();
    vec_4.plusBy(vec_1);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + vec_1.get(i), vec_4.get(i));
    TDoubleVector vec_5 = vec.clone();
    vec_5.plusBy(vec_1, 4);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + 4* vec_1.get(i), vec_5.get(i));

  }

  @Test
  public void sparseDoubleSortedComputeTest() {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);
    TDoubleVector vec_1 = genSparseDoubleSortedVector((int) 1.5 * nnz, dim);

    //dot
    double sum = 0.0;
    for (int i = 0; i < dim; i++)
      sum += vec.get(i) * vec_1.get(i);
    double dot = vec.dot(vec_1);
    assertEquals(sum, dot, 0.00000000000001);

    //plus
    TDoubleVector vec_2 = vec.plus(vec_1);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + vec_1.get(i), vec_2.get(i));
    TDoubleVector vec_3 = vec.plus(vec_1, 5);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + 5 * vec_1.get(i), vec_3.get(i));

    //plusBy
    TDoubleVector vec_4 = vec.clone();
    vec_4.plusBy(vec_1);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + vec_1.get(i), vec_4.get(i));
    TDoubleVector vec_5 = vec.clone();
    vec_5.plusBy(vec_1, 4);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + 4* vec_1.get(i), vec_5.get(i));
  }

  @Test
  public void sparseDummyComputeTest() {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);
    SparseDummyVector vec_1 = genSparseDummyVector(nnz, dim);
    int[] indices = vec_1.getIndices();

    //dot
    double sum = 0.0;
    for (int i = 0; i < dim; i++) {
      int pos = Arrays.binarySearch(indices, i);
      if (pos >= 0)
        sum += vec.get(i);
    }
    double dot = vec.dot(vec_1);
    assertEquals(sum, dot);

    //plusBy
    SparseDoubleVector vec_2 = (SparseDoubleVector) vec.clone();
    vec_2.plusBy(vec_1);

    for (int i = 0; i < dim; i++) {
      int pos = Arrays.binarySearch(indices, i);
      if (pos >= 0)
        assertEquals(vec.get(i) + 1, vec_2.get(i));
      else
        assertEquals(vec.get(i), vec_2.get(i));
    }

    SparseDoubleVector vec_3 = (SparseDoubleVector) vec.clone();
    vec_3.plusBy(vec_1, 5);
    for (int i = 0; i < dim; i++) {
      int pos = Arrays.binarySearch(indices, i);
      if (pos >= 0)
        assertEquals(vec.get(i) + 5, vec_3.get(i));
      else
        assertEquals(vec.get(i), vec_3.get(i));
    }

  }

  @Test
  public void filter() throws Exception {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);
    Random random = new Random();

    int oldSize = vec.size();
    for (int i = 0; i < 100; i++) {
      double val = random.nextDouble();
      vec.filter(val);

      if (vec.size() < 0.5 * oldSize) {
        for (int idx = 0; idx < dim; idx++)
          assertTrue(vec.get(idx) > val);
      }
    }
  }

  @Test
  public void getType() throws Exception {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);
    assertEquals(VectorType.T_DOUBLE_SPARSE, vec.getType());
  }

  @Test
  public void nonZeroNumber() throws Exception {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);
    int sum = 0;
    for (int i = 0; i < dim; i++) {
      if (vec.get(i) != 0)
        sum += 1;
    }

    long nnz = vec.nonZeroNumber();
    assertEquals(sum, nnz);
  }

  @Test
  public void size() throws Exception {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);
    int[] indexs = vec.getIndices();
    double[] values = vec.getValues();
    assertEquals(indexs.length, vec.size());
    assertEquals(values.length, vec.size());
  }

  @Test
  public void sparsity() throws Exception {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);
    int[] indexs = vec.getIndices();
    double[] values = vec.getValues();

    assertEquals((double) indexs.length / dim, vec.sparsity());
    assertEquals((double) values.length / dim, vec.sparsity());
  }

  @Test
  public void times() throws Exception {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);
    TDoubleVector vec_1 = vec.times(5.5);

    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) * 5.5, vec_1.get(i));
  }

  @Test
  public void timesBy() throws Exception {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);
    TDoubleVector vec_1 = vec.clone();

    vec.timesBy(5.5);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) * 5.5, vec.get(i));
  }

  @Test
  public void squaredNorm() throws Exception {
    SparseDoubleVector vec = genSparseDoubleVector(nnz, dim);
    double sum = 0.0;
    for (int i = 0; i < dim; i++)
      sum += vec.get(i) * vec.get(i);
    double squar = vec.squaredNorm();
    assertEquals(sum, squar, 0.0000000001);
  }

}