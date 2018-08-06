/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.DenseFloatVector;
import com.tencent.angel.ml.math.vector.SparseFloatVector;
import com.tencent.angel.ml.math.vector.TFloatVector;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class DenseFloatMatrixTest {
  @Test
  public void incTest() throws Exception {
    float[][] value = {{1.0f, 2.0f}, {3.0f, 4.0f}};
    DenseFloatMatrix mat = new DenseFloatMatrix(2, 2, value);

    mat.plusBy(0, 0, 1.0f);
    mat.plusBy(1, 1, 2.0f);

    assertEquals(2.0f, mat.get(0, 0));
    assertEquals(2.0f, mat.get(0, 1));
    assertEquals(3.0f, mat.get(1, 0));
    assertEquals(6.0f, mat.get(1, 1));

    DenseFloatMatrix mat_2 = new DenseFloatMatrix(2, 2);

    mat_2.plusBy(0, 0, 1.0f);
    mat_2.plusBy(0, 1, 2.0f);
    mat_2.plusBy(1, 0, 3.0f);
    mat_2.plusBy(1, 1, 4.0f);

    assertEquals(1.0f, mat_2.get(0, 0));
    assertEquals(2.0f, mat_2.get(0, 1));
    assertEquals(3.0f, mat_2.get(1, 0));
    assertEquals(4.0f, mat_2.get(1, 1));

    DenseFloatMatrix mat_3 = new DenseFloatMatrix(2, 2);

    mat_3.plusBy(0, 0, 1);
    mat_3.plusBy(0, 1, 3);
    mat_3.plusBy(1, 0, 5);
    mat_3.plusBy(1, 1, 7);

    assertEquals(1.0f, mat_3.get(0, 0));
    assertEquals(3.0f, mat_3.get(0, 1));
    assertEquals(5.0f, mat_3.get(1, 0));
    assertEquals(7.0f, mat_3.get(1, 1));
  }

  @Test
  public void plusByDenseFloatVectorTest() throws Exception {
    float[][] value = {{1.0f, 2.0f}, {3.0f, 4.0f}};
    DenseFloatMatrix mat = new DenseFloatMatrix(2, 2, value);
    TVector vec = new DenseFloatVector(2, new float[]{1.0f, 1.0f});
    vec.setRowId(0);

    mat.plusBy(vec);

    assertEquals(2.0f, mat.get(0, 0));
    assertEquals(3.0f, mat.get(0, 1));
    assertEquals(3.0f, mat.get(1, 0));
    assertEquals(4.0f, mat.get(1, 1));

    DenseFloatMatrix mat_1 = new DenseFloatMatrix(2, 2);
    DenseFloatVector vec_1 = new DenseFloatVector(2, new float[]{1.0f, 1.0f});
    vec_1.setRowId(0);

    mat_1.plusBy(vec_1);

    assertEquals(1.0f, mat_1.get(0, 0));
    assertEquals(1.0f, mat_1.get(0, 1));
    assertEquals(0.0f, mat_1.get(1, 0));
    assertEquals(0.0f, mat_1.get(1, 1));
  }

  @Test
  public void plusBySparseFloatVectorTest() throws Exception {
    float[][] value = {{1.0f, 2.0f}, {3.0f, 4.0f}};
    DenseFloatMatrix mat = new DenseFloatMatrix(2, 2,value);
    TVector vec = new SparseFloatVector(2);
    ((SparseFloatVector) vec).set(1, 1.0f);
    vec.setRowId(0);

    mat.plusBy(vec);

    assertEquals(1.0f, mat.get(0, 0));
    assertEquals(3.0f, mat.get(0, 1));
    assertEquals(3.0f, mat.get(1, 0));
    assertEquals(4.0f, mat.get(1, 1));
  }

//  @Test
//  public void plusBy3() throws Exception {
//    float[][] value = {{1.0f, 2.0f}, {3.0f, 4.0f}};
//    DenseFloatMatrix mat = new DenseFloatMatrix(2, 2,value);
//    TFloatVector vec = new DenseFloatVector(2, new float[]{1.0f, 1.0f});
//    vec.setRowId(0);
//    TDoubleVector vec_1 = new DenseDoubleVector(2, new double[]{1.0f, 1.0f});
//    vec_1.setRowId(1);
//    TDoubleVector vec_2 = new SparseDoubleVector(2);
//    vec_2.set(1, 1.0);
//    vec_2.setRowId(0);
//
//    mat.plusBy(vec);
//    mat.plusBy(vec_1);
//    mat.plusBy(vec_2);
//
//    assertEquals(2.0f, mat.get(0, 0));
//    assertEquals(4.0f, mat.get(0, 1));
//    assertEquals(4.0f, mat.get(1, 0));
//    assertEquals(5.0f, mat.get(1, 1));
//  }


  @Test
  public void get() throws Exception {
    float[][] value = {{1.0f, 2.0f}, {3.0f, 4.0f}};
    DenseFloatMatrix mat = new DenseFloatMatrix(2, 2, value);

    assertEquals(1.0f, mat.get(0, 0));
    assertEquals(2.0f, mat.get(0, 1));
    assertEquals(3.0f, mat.get(1, 0));
    assertEquals(4.0f, mat.get(1, 1));

    DenseFloatMatrix mat_1 = new DenseFloatMatrix(2, 2);
    DenseFloatVector vec = new DenseFloatVector(2, new float[]{1.0f, 2.0f});
    vec.setRowId(0);

    mat_1.plusBy(vec);

    assertEquals(1.0f, mat_1.get(0, 0));
    assertEquals(2.0f, mat_1.get(0, 1));
    assertEquals(0.0f, mat_1.get(1, 0));
    assertEquals(0.0f, mat_1.get(1, 1));
  }

  @Test
  public void getTVector() throws Exception {
    float[][] value = {{1.0f, 2.0f}, {3.0f, 4.0f}};
    DenseFloatMatrix mat = new DenseFloatMatrix(2, 2, value);

    TFloatVector vec = (TFloatVector) mat.getRow(0);

    assertEquals(0, vec.getRowId());
    assertEquals(1.0f, vec.get(0));
    assertEquals(2.0f, vec.get(1));
  }

  @Test
  public void sparsity() throws Exception {
    DenseFloatMatrix mat = new DenseFloatMatrix(2, 2, new float[][]{{0.0f, 1.0f}, {2.0f, 3.0f}});

    assertEquals(0.75, mat.sparsity());
  }

  @Test
  public void size() throws Exception {
    DenseFloatMatrix mat = new DenseFloatMatrix(2, 2, new float[][]{{0.0f, 1.0f}, {2.0f, 3.0f}});

    assertEquals(4, mat.size());
  }

  @Test
  public void clear() throws Exception {
    DenseFloatMatrix mat = new DenseFloatMatrix(2, 2, new float[][]{{0.0f, 1.0f}, {2.0f, 3.0f}});

    mat.clear();

    assertEquals(0.0f, mat.get(0, 0));
    assertEquals(0.0f, mat.get(0, 1));
    assertEquals(0.0f, mat.get(1, 0));
    assertEquals(0.0f, mat.get(1, 1));
  }

  @Test
  public void nonZeroNum() throws Exception {
    DenseFloatMatrix mat = new DenseFloatMatrix(2, 2, new float[][]{{0.0f, 1.0f}, {2.0f, 3.0f}});

    assertEquals(3, mat.nonZeroNum());

    DenseFloatMatrix mat_1 = new DenseFloatMatrix(2, 3);

    assertEquals(0, mat_1.nonZeroNum());
  }

}
