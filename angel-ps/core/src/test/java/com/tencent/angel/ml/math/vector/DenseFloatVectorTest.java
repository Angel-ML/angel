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
import com.tencent.angel.protobuf.generated.MLProtos;
import org.junit.Test;

import java.util.Random;

import static com.tencent.angel.ml.Utils.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class DenseFloatVectorTest {
  private static int dim = 1000;
  private static int nnz = 200;

  @Test
  public void clearTest() {
    DenseFloatVector vec = genDenseFloatVector(dim);

    vec.clear();

    for (float v: vec.getValues())
      assertEquals(0.0f, v);

  }

  @Test
  public void dotDenseFloatVectorTest() throws Exception {
    DenseFloatVector vec_1 = genDenseFloatVector(dim);
    TAbstractVector vec_2 = genDenseFloatVector(dim);

    double sum = 0.0f;
    for (int i = 0; i < dim; i++)
      sum += vec_1.getValues()[i] * ((DenseFloatVector)vec_2).getValues()[i];

    assertEquals(sum, vec_1.dot(vec_2));
  }

  @Test
  public void dotDenseDoubleVectorTest() throws Exception {
    DenseFloatVector vec_1 = genDenseFloatVector(dim);
    TAbstractVector vec_2 = genDenseDoubleVector(dim);

    double dot = 0.0;
    for (int i = 0; i < dim; i++)
      dot += vec_1.getValues()[i] * ((DenseDoubleVector)vec_2).getValues()[i];

    assertEquals(dot, vec_1.dot(vec_2));
  }


  @Test
  public void filterTest() throws Exception {
    DenseFloatVector vec = genDenseFloatVector(dim);
    Random random = new Random();

    for (int i = 0; i < 1000; i++) {
      double f = random.nextDouble();
      vec.filter(f);
      if (vec.size() < 0.5 * dim) {
        float[] values = vec.getValues();
        for (int j =0; j< values.length; j++)
          assertTrue(values[j] > f);
      }
    }
  }

  @Test
  public void getTest() throws Exception {
    float[] value = genFloatArray(dim);
    DenseFloatVector vec = new DenseFloatVector(dim, value);

    for (int i = 0; i < dim; i++)
      assertEquals(value[i], vec.get(i));
  }

  @Test
  public void getTypeTest() {
    DenseFloatVector vec = genDenseFloatVector(dim);
    assertEquals(MLProtos.RowType.T_FLOAT_DENSE, vec.getType());
  }

  @Test
  public void plusTest() throws Exception {
    DenseFloatVector vec_1 = genDenseFloatVector(dim);
    TAbstractVector vec_2 = genDenseFloatVector(dim);
    TAbstractVector vec_3 = genDenseFloatVector(dim);

    TFloatVector vec_4 = (TFloatVector)vec_1.plus(vec_2);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + ((DenseFloatVector)vec_2).get(i), vec_4.get(i));

    TFloatVector vec_5 = (TFloatVector)vec_1.plus(vec_3);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + (float)((DenseFloatVector)vec_3).get(i), vec_5.get(i));

    TFloatVector vec_6 = (TFloatVector)vec_1.plus(vec_2, 2.0f);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + 2.0f * ((DenseFloatVector)vec_2).get(i), vec_6.get(i));

    TFloatVector vec_7 = (TFloatVector)vec_1.plus(vec_3, 2.0);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + 2.0f * (float) ((DenseFloatVector)vec_3).get(i), vec_7.get(i));

    TFloatVector vec_8 = (TFloatVector)vec_1.plus(vec_3, 2);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + 2 * (float) ((DenseFloatVector)vec_3).get(i), vec_8.get(i));

  }

  @Test
  public void plusByDenseVectTest() throws Exception {
    DenseFloatVector vec_1 = genDenseFloatVector(dim);
    TAbstractVector vec_2 = genDenseFloatVector(dim);
    TAbstractVector vec_3 = genDenseFloatVector(dim);

    float[] oldValues = vec_1.getValues().clone();
    vec_1.plusBy(vec_2);
    for (int i = 0; i < dim; i++)
      assertEquals( oldValues[i] + ((DenseFloatVector)vec_2).get(i), vec_1.get(i));

    oldValues = vec_1.getValues().clone();
    vec_1.plusBy(vec_3);
    for (int i = 0; i < dim; i++)
      assertEquals(oldValues[i] + (float) ((DenseFloatVector) vec_3).get(i), vec_1.get(i));

    oldValues = vec_1.getValues().clone();
    vec_1.plusBy(vec_2, 3.0);
    for (int i = 0; i < dim; i++)
      assertEquals( oldValues[i] + 3.0f * ((DenseFloatVector)vec_2).get(i), vec_1.get(i));

    oldValues = vec_1.getValues().clone();
    vec_1.plusBy(vec_3, 4);
    for (int i = 0; i < dim; i++)
      assertEquals(oldValues[i] + 4.0f * (float) ((DenseFloatVector) vec_3).get(i), vec_1.get(i));
  }

  @Test
  public void plusSparseFloatTest() {
    DenseFloatVector vec = genDenseFloatVector(dim);
    SparseFloatVector vec_1 = genSparseFloatVector(nnz, dim);

    TFloatVector vec_2 = vec.clone();
    vec_2.plusBy(vec_1);
    for(int i = 0; i <dim; i++)
      assertEquals(vec_1.get(i) + vec.get(i), vec_2.get(i));
    TFloatVector vec_3 = vec.clone();
    vec_3.plusBy(vec_1, 5);
    for(int i = 0; i <dim; i++)
      assertEquals(5f * vec_1.get(i) + vec.get(i), vec_3.get(i));
  }

  @Test
  public void setTest() {
    DenseFloatVector vec = new DenseFloatVector(dim);
    int[] indexs = genSortedIndexs(nnz, dim);
    float[] vals = genFloatArray(nnz);
    for (int i = 0; i < nnz; i++)
      vec.set(indexs[i], vals[i]);

    for (int i = 0; i < nnz; i++)
      assertEquals(vals[i], vec.get(indexs[i]));

    indexs = genSortedIndexs(nnz, dim);
    double[] valsD = genDoubleArray(nnz);
    for (int i = 0; i < nnz; i++)
      vec.set(indexs[i], (float) valsD[i]);

    for (int i = 0; i < nnz; i++)
      assertEquals((float) valsD[i], vec.get(indexs[i]));
  }

  @Test
  public void sizeSquareTest() {
    DenseFloatVector vec = genDenseFloatVector(dim);
    assertEquals(dim, vec.size());

    float[] values = new float[dim];
    Random random = new Random();
    int nnzCount = 0;
    double square = 0.0;
    for (int i = 0; i < dim; i += random.nextInt(4) + 1) {
      float v = random.nextFloat();
      values[i] = v;
      square += v * v;
      nnzCount += 1;
    }

    DenseFloatVector vec_1 = new DenseFloatVector(dim, values);
    assertEquals(nnzCount, vec_1.nonZeroNumber());
    assertEquals(nnzCount/(double) dim, vec_1.sparsity());
    assertEquals(square, vec_1.squaredNorm());
  }

  @Test
  public void timestest() {
    DenseFloatVector vec = genDenseFloatVector(dim);
    float[] oldValues = vec.getValues().clone();

    DenseFloatVector vec_1 = (DenseFloatVector) vec.times(4.4);
    for (int i = 0; i < dim; i++)
      assertEquals(oldValues[i] * 4.4f, vec_1.get(i));

    vec.timesBy(5.5);
    for (int i = 0; i < dim; i++)
      assertEquals(oldValues[i] * 5.5f, vec.get(i));
  }
}
