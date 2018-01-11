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

package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.vector.SparseIntVector;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class SparseIntMatrixTest {
  @Test
  public void testPlusByGet() {
    SparseIntMatrix matrix = new SparseIntMatrix(2, 2);
    matrix.plusBy(0, 0, 1);
    matrix.plusBy(1, 1, 1);
    assertEquals(matrix.get(0, 0), 1);
    assertEquals(matrix.get(0, 1), 0);
    assertEquals(matrix.get(1, 0), 0);
    assertEquals(matrix.get(1, 1), 1);

    matrix.clear();
    SparseIntVector incVec = new SparseIntVector(2);
    incVec.set(0, 1);
    incVec.set(1, 1);
    incVec.setRowId(0);
    matrix.plusBy(incVec);
    assertEquals(matrix.get(0, 0), 1);
    assertEquals(matrix.get(0, 1), 1);
    assertEquals(matrix.get(1, 0), 0);
    assertEquals(matrix.get(1, 1), 0);

    matrix.clear();
    int [] rowIndexes = {0, 1};
    int [] colIndexes = {0, 1};
    int [] values = {1, 1};
    matrix.plusBy(rowIndexes, colIndexes, values);
    assertEquals(matrix.get(0, 0), 1);
    assertEquals(matrix.get(0, 1), 0);
    assertEquals(matrix.get(1, 0), 0);
    assertEquals(matrix.get(1, 1), 1);

    matrix.clear();
    colIndexes[0] = 0;
    colIndexes[1] = 1;
    values[0] = 1;
    values[1] = 1;
    matrix.plusBy(0, colIndexes, values);
    assertEquals(matrix.get(0, 0), 1);
    assertEquals(matrix.get(0, 1), 1);
    assertEquals(matrix.get(1, 0), 0);
    assertEquals(matrix.get(1, 1), 0);

    SparseIntMatrix matrix1 = new SparseIntMatrix(2, 2);
    matrix.clear();
    matrix.plusBy(0, 0, 1);
    matrix.plusBy(1, 1, 1);
    matrix1.plusBy(0, 0, 1);
    matrix1.plusBy(1, 1, 1);
    matrix.plusBy(matrix1);
    assertEquals(matrix.get(0, 0), 2);
    assertEquals(matrix.get(0, 1), 0);
    assertEquals(matrix.get(1, 0), 0);
    assertEquals(matrix.get(1, 1), 2);
    assertEquals(((SparseIntVector)matrix.getRow(0)).get(0), 2);
    assertEquals(((SparseIntVector)matrix.getRow(0)).get(1), 0);
    assertEquals(((SparseIntVector)matrix.getRow(1)).get(0), 0);
    assertEquals(((SparseIntVector)matrix.getRow(1)).get(1), 2);
  }

  @Test
  public void testSizeSparsity() {
    SparseIntMatrix matrix= new SparseIntMatrix(2, 2);
    matrix.plusBy(0, 0, 1);
    matrix.plusBy(1, 1, 1);
    assertEquals(matrix.size(), 4);
    assertEquals(matrix.sparsity(), 0.5);
  }
}
