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

import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;


public class DenseDoubleMatrixTest {
  @Test
  public void inc() throws Exception {
    double[][] value = {{1.0, 2.0},{3.0, 4.0}};
    DenseDoubleMatrix mat = new DenseDoubleMatrix(2, 2, value);

    mat.plusBy(0, 0, 1);
    mat.plusBy(0, 1, 1);
    mat.plusBy(1, 1, 1);

    assertEquals(2.0, mat.get(0, 0));
    assertEquals(3.0, mat.get(0, 1));
    assertEquals(5.0, mat.get(1, 1));
  }

  @Test
  public void plusBy1() throws Exception {
    double[][] value = {{1.0, 2.0},{3.0, 4.0}};
    DenseDoubleMatrix mat = new DenseDoubleMatrix(2, 2, value);
    TDoubleVector vec = new DenseDoubleVector(2, new double[]{1.0, 1.0});
    vec.setRowId(0);
    TDoubleVector vec_1 = new SparseDoubleVector(2, new int[]{1}, new double[]{1.0});
    vec_1.setRowId(1);

    mat.plusBy(vec);
    mat.plusBy(vec_1);

    assertEquals(2.0, mat.get(0, 0));
    assertEquals(3.0, mat.get(0, 1));
    assertEquals(3.0, mat.get(1, 0));
    assertEquals(5.0, mat.get(1, 1));
  }

  @Test
  public void plusBy2() throws Exception {
    double[][] value = {{1.0, 2.0},{3.0, 4.0}};
    DenseDoubleMatrix mat = new DenseDoubleMatrix(2, 2, value);
    DenseDoubleVector vec = new DenseDoubleVector(2, new double[]{1.0, 1.0});
    vec.setRowId(0);

    mat.plusBy(vec);

    assertEquals(2.0, mat.get(0, 0));
    assertEquals(3.0, mat.get(0, 1));
    assertEquals(3.0, mat.get(1, 0));

  }

  @Test
  public void plusBy3() throws Exception {
    double[][] value = {{1.0, 2.0},{3.0, 4.0}};
    DenseDoubleMatrix mat = new DenseDoubleMatrix(2, 2, value);
    SparseDoubleVector vec = new SparseDoubleVector(2);
    vec.setRowId(0);
    vec.set(0, 1.0);
    SparseDoubleVector vec_1 = new SparseDoubleVector(2);
    vec_1.setRowId(1);
    vec_1.set(1, 1.0);

    mat.plusBy(vec);
    mat.plusBy(vec_1);

    assertEquals(2.0, mat.get(0, 0));
    assertEquals(2.0, mat.get(0, 1));
    assertEquals(3.0, mat.get(1, 0));
    assertEquals(5.0, mat.get(1, 1));
  }

  @Test
  public void get() throws Exception {
    double[][] value = {{1.0, 2.0},{3.0, 4.0}};
    DenseDoubleMatrix mat = new DenseDoubleMatrix(2, 2, value);

    double v1 = mat.get(0, 0);
    double v2 = mat.get(1, 1);
    assertEquals(1.0, v1);
    assertEquals(4.0, v2);
  }

  @Test
  public void getTDoubleVector() throws Exception {
    double[][] value = {{1.0, 2.0},{3.0, 4.0}};
    DenseDoubleMatrix mat = new DenseDoubleMatrix(2, 2, value);

    TDoubleVector vec = (TDoubleVector)mat.getRow(0);

    assertEquals(2, vec.size());
    assertEquals(0, vec.getRowId());
    assertEquals(1.0, vec.get(0));
    assertEquals(2.0, vec.get(1));

    DenseDoubleMatrix mat_1 = new DenseDoubleMatrix(2, 2, new double[2][2]);
    TDoubleVector vec_1 = new DenseDoubleVector(2, new double[]{1.0, 2.0});
    vec_1.setRowId(0);
    mat_1.plusBy(vec_1);

    TDoubleVector vec_2 = (TDoubleVector)mat_1.getRow(0);

    assertEquals(1.0, vec_2.get(0));
    assertEquals(2.0, vec_2.get(1));
  }

  @Test
  public void sparsity() throws Exception {
    double[][] value = {{0.0, 0.0}, {1.0, 0.0}};
    DenseDoubleMatrix mat = new DenseDoubleMatrix(2, 2, value);

    double s = mat.sparsity();
    assertEquals(0.25, s);
  }

  @Test
  public void size() throws Exception {
    double[][] value = {{0.0, 0.0}, {1.0, 0.0}};
    DenseDoubleMatrix mat = new DenseDoubleMatrix(2, 2, value);

    long size = mat.size();
    assertEquals(4, size);
  }


  @Test
  public void clear() {
    double[][] value = {{0.0, 0.0}, {1.0, 0.0}};
    DenseDoubleMatrix mat = new DenseDoubleMatrix(2, 2, value);

    mat.clear();

    assertEquals(0.0, mat.get(0, 0));
    assertEquals(0.0, mat.get(0, 1));
    assertEquals(0.0, mat.get(1, 0));
    assertEquals(0.0, mat.get(1, 1));
  }

  @Test
  public void nonZeroNum() {
    double[][] value = {{0.0, 0.0}, {1.0, 0.0}};
    DenseDoubleMatrix mat = new DenseDoubleMatrix(2, 2, value);

    long nnz = mat.nonZeroNum();
    assertEquals(1, mat.nonZeroNum());
  }

}
