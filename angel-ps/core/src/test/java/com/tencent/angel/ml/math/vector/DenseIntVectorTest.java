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

import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.VectorType;
import com.tencent.angel.protobuf.generated.MLProtos;
import org.junit.Test;

import java.util.Random;

import static junit.framework.Assert.assertEquals;

public class DenseIntVectorTest {
  @Test
  public void cloneTest() throws Exception {
    int dim = 1000;
    Random random = new Random();

    int[] values_1 = new int[dim];
    for (int i = 0; i < dim; i++)
      values_1[i] = random.nextInt();

    DenseIntVector vec_1= new DenseIntVector(dim, values_1);
    vec_1.setMatrixId(1);
    vec_1.setRowId(2);

    DenseIntVector vec_2 = (DenseIntVector) vec_1.clone();

    for (int i = 0; i < dim; i++)
      assertEquals(vec_2.get(i), vec_1.get(i));


    DenseIntVector vec_3 = new DenseIntVector(dim);

    vec_3.clone(vec_1);

    for (int i = 0; i < dim; i++)
      assertEquals(vec_2.get(i), vec_1.get(i));

  }

  @Test
  public void getAndSet() throws Exception {
    int dim = 1000;
    Random random = new Random();

    int[] values = new int[dim];
    for (int i = 0; i < dim; i++)
      values[i] = random.nextInt();

    DenseIntVector vec= new DenseIntVector(dim);

    for (int i = 0; i < dim; i++)
      vec.set(i, values[i]);


    for (int i = 0; i < dim; i++)
      assertEquals(values[i], vec.get(i));
  }


  @Test
  public void inc() throws Exception {
    int dim = 1000;
    Random random = new Random();

    int[] values = new int[dim];
    for (int i = 0; i < dim; i++)
      values[i] = random.nextInt();

    DenseIntVector vec= new DenseIntVector(dim, values);

    for (int i = 0; i < 100; i++) {
      int idx = random.nextInt(dim);
      int delt = random.nextInt();
      int old = vec.get(idx);

      vec.plusBy(idx, delt);

      assertEquals(old + delt, vec.get(idx));
    }
  }

  @Test
  public void getValues() throws Exception {
    int dim = 1000;
    Random random = new Random();

    int[] values = new int[dim];
    for (int i = 0; i < dim; i++)
      values[i] = random.nextInt();

    DenseIntVector vec= new DenseIntVector(dim, values);

    int[] getValues = vec.getValues();

    assert (getValues.equals(values));
  }


  @Test
  public void plusBy() throws Exception {
    int dim = 1000;
    Random random = new Random();

    int[] values_1 = new int[dim];
    for (int i = 0; i < dim; i++)
      values_1[i] = random.nextInt();

    DenseIntVector vec_1= new DenseIntVector(dim, values_1);

    int[] values_2 = new int[dim];
    for (int i = 0; i < dim; i++)
      values_2[i] = random.nextInt();

    TAbstractVector vec_2= new DenseIntVector(dim, values_2);
    int[] oldValues = new int[dim];
    for (int i = 0; i < dim; i++)
      oldValues[i] = vec_1.get(i);

    vec_1.plusBy(vec_2);

    for(int i = 0; i < dim; i++)
      assertEquals(oldValues[i] + values_2[i], vec_1.get(i));

    for (int i = 0; i < dim; i++)
      oldValues[i] = vec_1.get(i);

    vec_1.plusBy(vec_2, 3);

    for(int i = 0; i < dim; i++)
      assertEquals(oldValues[i] + 3 * values_2[i], vec_1.get(i));

  }

  @Test
  public void sparsity() throws Exception {
    int[] values = new int[]{1, 2, 3, 0, 0, 4, 5, 0, 0, 7};
    DenseIntVector vec = new DenseIntVector(10, values);

    assertEquals(0.6, vec.sparsity());
  }

  @Test
  public void getType() throws Exception {
    int[] values = new int[]{1, 2, 3, 0, 0, 4, 5, 0, 0, 7};
    DenseIntVector vec = new DenseIntVector(10, values);

    assertEquals(MLProtos.RowType.T_INT_DENSE, vec.getType());
  }

  @Test
  public void size() throws Exception {
    int[] values = new int[]{1, 2, 3, 0, 0, 4, 5, 0, 0, 7};
    DenseIntVector vec = new DenseIntVector(10, values);

    assertEquals(10, vec.size());
  }

  @Test
  public void clear() throws Exception {
    int[] values = new int[]{1, 2, 3, 0, 0, 4, 5, 0, 0, 7};
    DenseIntVector vec = new DenseIntVector(10, values);

    vec.clear();

    for (int i = 0; i < 10; i++)
      assertEquals(0, vec.get(i));
  }

  @Test
  public void nonZeroNumber() throws Exception {
    int[] values = new int[]{1, 2, 3, 0, 0, 4, 5, 0, 0, 7};
    DenseIntVector vec = new DenseIntVector(10, values);

    assertEquals(6, vec.nonZeroNumber());
  }


  @Test
  public void add() throws Exception {
    int dim = 1000;
    Random random = new Random();

    int[] values = new int[dim];
    for (int i = 0; i < dim; i++)
      values[i] = random.nextInt();

    DenseIntVector vec= new DenseIntVector(dim, values);

    for (int i = 0; i < 100; i++) {
      int idx = random.nextInt(dim);
      int old = vec.get(idx);
      int delt = random.nextInt();

      vec.plusBy(idx, delt);

      assertEquals(old + delt, vec.get(idx));
    }
  }

}
