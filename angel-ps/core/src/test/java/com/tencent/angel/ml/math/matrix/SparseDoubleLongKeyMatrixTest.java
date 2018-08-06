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

import com.tencent.angel.ml.math.vector.SparseLongKeyDoubleVector;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class SparseDoubleLongKeyMatrixTest {
  @Test
  public void testPlusByGet() {
    SparseDoubleLongKeyMatrix matrix = new SparseDoubleLongKeyMatrix(2, -1);
    matrix.plusBy(0, -100, 1.0);
    matrix.plusBy(1, 100, 1.0);
    assertEquals(matrix.get(0, -100), 1.0);
    assertEquals(matrix.get(1, 100), 1.0);

    matrix.clear();
    SparseLongKeyDoubleVector incVec = new SparseLongKeyDoubleVector(-1);
    incVec.set(-100, 1);
    incVec.set(100, 1);
    incVec.setRowId(0);
    matrix.plusBy(incVec);
    assertEquals(matrix.get(0, -100), 1.0);
    assertEquals(matrix.get(0, 100), 1.0);

    matrix.clear();
    int [] rowIndexes = {0, 1};
    long [] colIndexes = {-100, 100};
    double [] values = {1.0, 1.0};
    matrix.plusBy(rowIndexes, colIndexes, values);
    assertEquals(matrix.get(0, -100), 1.0);
    assertEquals(matrix.get(1, 100), 1.0);

    matrix.clear();
    colIndexes[0] = -100;
    colIndexes[1] = 100;
    values[0] = 1.0;
    values[1] = 1.0;
    matrix.plusBy(0, colIndexes, values);
    assertEquals(matrix.get(0, -100), 1.0);
    assertEquals(matrix.get(0, 100), 1.0);

    SparseDoubleLongKeyMatrix matrix1 = new SparseDoubleLongKeyMatrix(2, -1);
    matrix.clear();
    matrix.plusBy(0, -100, 1.0);
    matrix.plusBy(1, 100, 1.0);
    matrix1.plusBy(0, -100, 1.0);
    matrix1.plusBy(1, 100, 1.0);
    matrix.plusBy(matrix1);
    assertEquals(((SparseLongKeyDoubleVector)matrix.getRow(0)).get(-100), 2.0);
    assertEquals(((SparseLongKeyDoubleVector)matrix.getRow(1)).get(100), 2.0);
  }

  @Test
  public void testSizeSparsity() {
    SparseDoubleLongKeyMatrix matrix = new SparseDoubleLongKeyMatrix(2, -1);
    matrix.plusBy(0, -100, 1.0);
    matrix.plusBy(1, 100, 1.0);
    assertEquals(matrix.size(), -2);
    assertEquals(matrix.nonZeroNum(), 2);
    assertEquals(matrix.sparsity(), 5.421010862427522E-20);

    SparseDoubleLongKeyMatrix matrix1= new SparseDoubleLongKeyMatrix(2, 2);
    matrix1.plusBy(0, -100, 1.0);
    matrix1.plusBy(1, 100, 1.0);
    assertEquals(matrix1.size(), 4);
    assertEquals(matrix1.nonZeroNum(), 2);
    assertEquals(matrix1.sparsity(), 0.5);
  }
}
