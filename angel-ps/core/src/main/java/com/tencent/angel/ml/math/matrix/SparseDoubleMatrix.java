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

import com.tencent.angel.ml.math.vector.DenseFloatVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Sparse double matrix that is represented by a group of sparse double vector {@link SparseDoubleVector}
 */
public class SparseDoubleMatrix extends TDoubleMatrix<SparseDoubleVector> {
  private static final Log LOG = LogFactory.getLog(SparseDoubleMatrix.class);

  /**
   * Build a SparseDoubleMatrix
   * @param row row number
   * @param col column number
   */
  public SparseDoubleMatrix(int row, int col) {
    this(row, col, new SparseDoubleVector[row]);
  }

  public SparseDoubleMatrix(int row, int col, SparseDoubleVector[] vectors) {
    super(row, col, vectors);
  }

  /**
   * Init the empty vector
   *
   * @param rowIndex row index
   * @return
   */
  @Override
  public SparseDoubleVector initVector(int rowIndex) {
    SparseDoubleVector ret = new SparseDoubleVector((int)col);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }
}
