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
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.ml.math.vector.TFloatVector;
import com.tencent.angel.ml.math.VectorType;
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
  public void cloneTest() throws Exception {
    DenseFloatVector vec = genDenseFloatVector(dim);
    vec.setMatrixId(1);
    vec.setRowId(2);

    DenseFloatVector vec_1 = (DenseFloatVector) vec.clone();

    assertEquals(vec_1.getMatrixId(), vec.getMatrixId());
    assertEquals(vec_1.getRowId(), vec.getRowId());
    for (int i = 0; i < vec.size(); i++)
      assertEquals(vec_1.get(i), vec.get(i));

    TFloatVector vec_2 = new DenseFloatVector();
    vec_2.clone(vec);

    assertEquals(vec_2.getMatrixId(), vec.getMatrixId());
    assertEquals(vec_2.getRowId(), vec.getRowId());
    for (int i = 0; i < vec.size(); i++)
      assertEquals(vec_2.get(i), vec.get(i));
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
    assertEquals(VectorType.T_FLOAT_DENSE, vec.getType());
  }

  @Test
  public void plusTest() throws Exception {
    DenseFloatVector vec_1 = genDenseFloatVector(dim);
    TAbstractVector vec_2 = genDenseFloatVector(dim);
    TAbstractVector vec_3 = genDenseDoubleVector(dim);

    TFloatVector vec_4 = vec_1.plus(vec_2);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + ((DenseFloatVector)vec_2).get(i), vec_4.get(i));

    TFloatVector vec_5 = vec_1.plus(vec_3);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + (float)((DenseDoubleVector)vec_3).get(i), vec_5.get(i));

    TFloatVector vec_6 = vec_1.plus(vec_2, 2.0f);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + 2.0f * ((DenseFloatVector)vec_2).get(i), vec_6.get(i));

    TFloatVector vec_7 = vec_1.plus(vec_3, 2.0);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + 2.0f * (float) ((DenseDoubleVector)vec_3).get(i), vec_7.get(i));

    TFloatVector vec_8 = vec_1.plus(vec_3, 2);
    for (int i = 0; i < dim; i++)
      assertEquals(vec_1.get(i) + 2 * (float) ((DenseDoubleVector)vec_3).get(i), vec_8.get(i));

  }

  @Test
  public void plusByDenseVectTest() throws Exception {
    DenseFloatVector vec_1 = genDenseFloatVector(dim);
    TAbstractVector vec_2 = genDenseFloatVector(dim);
    TAbstractVector vec_3 = genDenseDoubleVector(dim);

    float[] oldValues = vec_1.getValues().clone();
    vec_1.plusBy(vec_2);
    for (int i = 0; i < dim; i++)
      assertEquals( oldValues[i] + ((DenseFloatVector)vec_2).get(i), vec_1.get(i));

    oldValues = vec_1.getValues().clone();
    vec_1.plusBy(vec_3);
    for (int i = 0; i < dim; i++)
      assertEquals(oldValues[i] + (float) ((DenseDoubleVector) vec_3).get(i), vec_1.get(i));

    oldValues = vec_1.getValues().clone();
    vec_1.plusBy(vec_2, 3.0);
    for (int i = 0; i < dim; i++)
      assertEquals( oldValues[i] + 3.0f * ((DenseFloatVector)vec_2).get(i), vec_1.get(i));

    oldValues = vec_1.getValues().clone();
    vec_1.plusBy(vec_3, 4);
    for (int i = 0; i < dim; i++)
      assertEquals(oldValues[i] + 4.0f * (float) ((DenseDoubleVector) vec_3).get(i), vec_1.get(i));
  }

  @Test
  public void plusBySparseVectTest() throws Exception {
    DenseFloatVector vec_1 = genDenseFloatVector(dim);
    TAbstractVector vec_2 = genSparseDoubleVector(nnz, dim);
    TAbstractVector vec_3 = genSparseDoubleSortedVector(nnz, dim);

    float[] oldValues = vec_1.getValues().clone();

    oldValues = vec_1.getValues().clone();
    vec_1.plusBy(vec_2, 3.0);
    for (int i = 0; i < dim; i++)
      assertEquals( oldValues[i] + 3.0f * (float) ((SparseDoubleVector)vec_2).get(i), vec_1.get(i));

    oldValues = vec_1.getValues().clone();
    vec_1.plusBy(vec_3, 4.0);
    for (int i = 0; i < 10; i++)
      assertEquals(oldValues[i] + 4.0f * (float) ((SparseDoubleSortedVector) vec_3).get(i), vec_1.get(i));
  }

//  @Test
//  public void plusByArrayTest() {
//    DenseFloatVector vec = new DenseFloatVector(dim);
//    int[] index = genSortedIndexs(nnz, dim);
//    float[] deltF = genFloatArray(nnz);
//    double[] deltD = genDoubleArray(nnz);
//
//    float[] oldVal = vec.getValues().clone();
//    vec.plusBy(index, deltF);
//    for (int i = 0; i < nnz; i++) {
//      int idx = index[i];
//      assertEquals(oldVal[idx] + deltF[i], vec.get(idx));
//    }
//
//    oldVal = vec.getValues().clone();
//    vec.plusBy(index, deltD);
//    for (int i = 0; i < nnz; i++) {
//      int idx = index[i];
//      assertEquals(oldVal[idx] + (float) deltD[i], vec.get(idx));
//    }
//
//    oldVal = vec.getValues().clone();
//    vec.plusBy(index, deltF);
//    for (int i = 0; i < nnz; i++) {
//      int idx = index[i];
//      assertEquals(oldVal[idx] + deltF[i], vec.get(idx));
//    }
//
//    oldVal = vec.getValues().clone();
//    vec.plusBy(index, deltD);
//    for (int i = 0; i < nnz; i++) {
//      int idx = index[i];
//      assertEquals(oldVal[idx] + (float) deltD[i], vec.get(idx));
//    }
//  }

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
      vec.set(indexs[i], valsD[i]);

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
