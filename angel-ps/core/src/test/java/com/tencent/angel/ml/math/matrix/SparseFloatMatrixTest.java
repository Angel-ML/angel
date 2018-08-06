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

import com.tencent.angel.ml.math.vector.SparseFloatVector;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class SparseFloatMatrixTest {
  @Test
  public void testPlusByGet() {
    SparseFloatMatrix matrix = new SparseFloatMatrix(2, 2);
    matrix.plusBy(0, 0, 1.0f);
    matrix.plusBy(1, 1, 1.0f);
    assertEquals(matrix.get(0, 0), 1.0f);
    assertEquals(matrix.get(0, 1), 0.0f);
    assertEquals(matrix.get(1, 0), 0.0f);
    assertEquals(matrix.get(1, 1), 1.0f);

    matrix.clear();
    SparseFloatVector incVec = new SparseFloatVector(2);
    incVec.set(0, 1);
    incVec.set(1, 1);
    incVec.setRowId(0);
    matrix.plusBy(incVec);
    assertEquals(matrix.get(0, 0), 1.0f);
    assertEquals(matrix.get(0, 1), 1.0f);
    assertEquals(matrix.get(1, 0), 0.0f);
    assertEquals(matrix.get(1, 1), 0.0f);

    matrix.clear();
    int [] rowIndexes = {0, 1};
    int [] colIndexes = {0, 1};
    float [] values = {1.0f, 1.0f};
    matrix.plusBy(rowIndexes, colIndexes, values);
    assertEquals(matrix.get(0, 0), 1.0f);
    assertEquals(matrix.get(0, 1), 0.0f);
    assertEquals(matrix.get(1, 0), 0.0f);
    assertEquals(matrix.get(1, 1), 1.0f);

    matrix.clear();
    colIndexes[0] = 0;
    colIndexes[1] = 1;
    values[0] = 1.0f;
    values[1] = 1.0f;
    matrix.plusBy(0, colIndexes, values);
    assertEquals(matrix.get(0, 0), 1.0f);
    assertEquals(matrix.get(0, 1), 1.0f);
    assertEquals(matrix.get(1, 0), 0.0f);
    assertEquals(matrix.get(1, 1), 0.0f);

    SparseFloatMatrix matrix1 = new SparseFloatMatrix(2, 2);
    matrix.clear();
    matrix.plusBy(0, 0, 1.0f);
    matrix.plusBy(1, 1, 1.0f);
    matrix1.plusBy(0, 0, 1.0f);
    matrix1.plusBy(1, 1, 1.0f);
    matrix.plusBy(matrix1);
    assertEquals(matrix.get(0, 0), 2.0f);
    assertEquals(matrix.get(0, 1), 0.0f);
    assertEquals(matrix.get(1, 0), 0.0f);
    assertEquals(matrix.get(1, 1), 2.0f);
    assertEquals(((SparseFloatVector)matrix.getRow(0)).get(0), 2.0f);
    assertEquals(((SparseFloatVector)matrix.getRow(0)).get(1), 0.0f);
    assertEquals(((SparseFloatVector)matrix.getRow(1)).get(0), 0.0f);
    assertEquals(((SparseFloatVector)matrix.getRow(1)).get(1), 2.0f);
  }

  @Test
  public void testSizeSparsity() {
    SparseFloatMatrix matrix= new SparseFloatMatrix(2, 2);
    matrix.plusBy(0, 0, 1.0f);
    matrix.plusBy(1, 1, 1.0f);
    assertEquals(matrix.size(), 4);
    assertEquals(matrix.sparsity(), 0.5);
  }
}
