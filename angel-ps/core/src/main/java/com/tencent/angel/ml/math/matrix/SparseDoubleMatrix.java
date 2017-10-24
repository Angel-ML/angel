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

import com.tencent.angel.ml.math.vector.SparseIntDoubleVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Sparse double matrix that is represented by a group of sparse double vector {@link SparseIntDoubleVector}
 */
public class SparseDoubleMatrix extends TDoubleMatrix {
  private static final Log LOG = LogFactory.getLog(SparseDoubleMatrix.class);
  private final SparseIntDoubleVector[] vectors;

  /**
   * Build a SparseDoubleMatrix
   * @param row row number
   * @param col column number
   */
  public SparseDoubleMatrix(int row, int col) {
    super(row, col);
    vectors = new SparseIntDoubleVector[row];
  }

  /**
   * Init the empty vector
   *
   * @param rowIndex row index
   * @return
   */
  @Override
  public SparseIntDoubleVector initVector(int rowIndex) {
    SparseIntDoubleVector ret = new SparseIntDoubleVector((int)col);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }
}
