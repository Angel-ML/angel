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

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.protobuf.generated.MLProtos;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static com.tencent.angel.ml.Utils.genDenseFloatVector;
import static com.tencent.angel.ml.Utils.genSparseFloatVector;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class SparseFloatVectorTest {
  private static int dim = 100;
  private static int cap = 20;
  private static int nnz = 30;

  @Test
  public void cloneTest() throws Exception {
    SparseFloatVector vec = new SparseFloatVector(300, 10000);

    SparseFloatVector vec_1 = (SparseFloatVector) vec.clone();

    int[] index = vec.getIndices();
    int[] index1 = vec.getIndices();
    Arrays.sort(index);
    Arrays.sort(index1);
    assertArrayEquals(index, index1);
    for (int i = 0; i < index.length; i++)
      assertEquals(vec.get(i), vec_1.get(i));

    TFloatVector vec_2 = new SparseFloatVector(300, 500);
    vec_2.clone(vec);
    int[] index2 = vec_2.getIndices();
    Arrays.sort(index2);
    assertArrayEquals(index, index2);
    for (int i = 0; i < index.length; i++)
      assertEquals(vec.get(i), vec_2.get(i));
  }

  @Test
  public void denseFloatComputeTest() throws Exception {
    int dim = 10;
    int nnz = 4;
    SparseFloatVector vec = genSparseFloatVector(nnz, dim);

    TFloatVector vec_1 = genDenseFloatVector(dim);

    // dot
    double sum = 0.0;
    for (int i = 0; i < dim; i++)
      sum += vec_1.get(i) * vec.get(i);
    double dot = vec.dot(vec_1);
    assertEquals(sum, dot);

    // plus
    TVector vec_2 = vec.plus(vec_1);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + vec.get(i), ((TFloatVector) vec_2).get(i));
    TVector vec_3 = vec.plus(vec_1, 3);
    for (int i = 0; i < 10; i++)
      System.out.println(vec.get(i) + ", " + vec_1.get(i) + "," + ((TFloatVector) vec_3).get(i));

    for (int i = 0; i < dim; i++)
      assertEquals(3 * vec_1.get(i) + vec.get(i), ((TFloatVector) vec_3).get(i));

    // plusBy
    TFloatVector vec_5 = vec.clone();
    vec_5.plusBy(vec_1);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + vec_1.get(i), vec_5.get(i));
    TFloatVector vec_6 = vec.clone();
    vec_6.plusBy(vec_1, 4);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + 4 * vec_1.get(i), vec_6.get(i));

  }

  @Test
  public void sparseFloatComputeTest() {
    SparseFloatVector vec = genSparseFloatVector(nnz, dim);
    TFloatVector vec_1 = genSparseFloatVector(nnz, dim);

    // dot
    double sum = 0.0;
    for (int i = 0; i < dim; i++)
      sum += vec.get(i) * vec_1.get(i);
    double dot = vec.dot(vec_1);
    assertEquals(sum, dot, 0.00000000001);

    // plus
    TFloatVector vec_2 = (TFloatVector) vec.plus(vec_1);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + vec_1.get(i), vec_2.get(i));
    TFloatVector vec_3 = (TFloatVector) vec.plus(vec_1, 4);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + 4 * vec_1.get(i), vec_3.get(i));

    // plusBy
    TFloatVector vec_4 = vec.clone();
    vec_4.plusBy(vec_1);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + vec_1.get(i), vec_4.get(i));
    TFloatVector vec_5 = vec.clone();
    vec_5.plusBy(vec_1, 4);
    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) + 4 * vec_1.get(i), vec_5.get(i));

  }

  @Test
  public void filter() throws Exception {
    SparseFloatVector vec = genSparseFloatVector(nnz, dim);
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
    SparseFloatVector vec = genSparseFloatVector(nnz, dim);
    assertEquals(MLProtos.RowType.T_FLOAT_SPARSE, vec.getType());
  }

  @Test
  public void nonZeroNumber() throws Exception {
    SparseFloatVector vec = genSparseFloatVector(nnz, dim);
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
    SparseFloatVector vec = genSparseFloatVector(nnz, dim);
    int[] indexs = vec.getIndices();
    float[] values = vec.getValues();
    assertEquals(indexs.length, vec.size());
    assertEquals(values.length, vec.size());
  }

  @Test
  public void sparsity() throws Exception {
    SparseFloatVector vec = genSparseFloatVector(nnz, dim);
    int[] indexs = vec.getIndices();
    float[] values = vec.getValues();

    assertEquals((double) indexs.length / dim, vec.sparsity());
    assertEquals((double) values.length / dim, vec.sparsity());
  }

  @Test
  public void times() throws Exception {
    SparseFloatVector vec = genSparseFloatVector(nnz, dim);
    TFloatVector vec_1 = (TFloatVector) vec.times(5.5);

    for (int i = 0; i < dim; i++)
      assertEquals(vec.get(i) * 5.5f, vec_1.get(i));
  }

  @Test
  public void timesBy() throws Exception {
    SparseFloatVector vec = genSparseFloatVector(nnz, dim);
    TFloatVector vec_1 = vec.clone();

    vec.timesBy(5.5);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) * 5.5f, vec.get(i));
  }

  @Test
  public void squaredNorm() throws Exception {
    SparseFloatVector vec = genSparseFloatVector(nnz, dim);
    double sum = 0.0;
    for (int i = 0; i < dim; i++)
      sum += vec.get(i) * vec.get(i);
    double squar = vec.squaredNorm();
    assertEquals(sum, squar, 0.0000000001);
  }

}
