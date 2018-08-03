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

/**
 * Sparse float matrix that is represented by a group of sparse float vector {@link SparseFloatVector}
 */
public class SparseFloatMatrix extends TFloatMatrix<SparseFloatVector>{
  /**
   * Create a new float matrix.
   *
   * @param row the row
   * @param col the col
   */
  public SparseFloatMatrix(int row, int col) {
    super(row, col, new SparseFloatVector[row]);
  }

  public SparseFloatMatrix(int row, int col, SparseFloatVector[] vectors) {
    super(row, col, vectors);
  }

  @Override public SparseFloatVector initVector(int rowIndex) {
    SparseFloatVector ret = new SparseFloatVector((int)col);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }
}
