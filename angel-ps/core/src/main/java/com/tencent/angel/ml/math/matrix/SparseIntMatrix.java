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

package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.TMatrix;
import com.tencent.angel.ml.math.vector.SparseIntVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Sparse int matrix that is represented by a group of sparse int vector {@link SparseIntVector}
 */
public class SparseIntMatrix extends TIntMatrix<SparseIntVector> {

  private static final Log LOG = LogFactory.getLog(SparseIntMatrix.class);

  /**
   * Create a new empty matrix.
   *
   * @param row the row number
   * @param col the col number
   */
  public SparseIntMatrix(int row, int col) {
    super(row, col, new SparseIntVector[row]);
  }

  public SparseIntMatrix(int row, long col, SparseIntVector[] vectors) {
    super(row, col, vectors);
  }

  /**
   * init the empty vector
   *
   * @param rowIndex row index
   * @return
   */
  public SparseIntVector initVector(int rowIndex) {
    SparseIntVector ret = new SparseIntVector((int)col);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }

  @Override
  public TMatrix plusBy(TMatrix other) {
    if (other instanceof COOIntMatrix) {
      return plusBy((COOIntMatrix) other);
    } else {
      return super.plusBy(other);
    }
  }

  private TMatrix plusBy(COOIntMatrix other) {
    return plusBy(other.rowIds, other.colIds, other.values);
  }

}
