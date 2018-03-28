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

import com.tencent.angel.ml.math.vector.DenseFloatVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Dense double matrix that is represented by a group of dense float vector {@link DenseFloatVector}
 */
public class DenseFloatMatrix extends TFloatMatrix<DenseFloatVector>  {
  private static final Log LOG = LogFactory.getLog(DenseFloatMatrix.class);

  /**
   * Create a new empty matrix.
   *
   * @param row the row
   * @param col the col
   */
  public DenseFloatMatrix(int row, int col) {
    this(row, col, new DenseFloatVector[row]);
  }

  public DenseFloatMatrix(int row, int col, DenseFloatVector[] vectors) {
    super(row, col, vectors);
  }

  /**
   * Create a new matrix with <code>values</code>.
   *
   * @param row    the row
   * @param col    the col
   * @param values the values
   */
  public DenseFloatMatrix(int row, int col, float[][] values) {
    this(row, col);
    if (values != null) {
      assert (row == values.length);
      for (int i = 0; i < row; i++) {
        vectors[i] = initVector(i, values[i]);
      }
    }
  }

  /**
   * init the empty vector
   *
   * @param rowIndex
   * @return
   */
  public DenseFloatVector initVector(int rowIndex) {
    DenseFloatVector ret = new DenseFloatVector((int)col);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }

  public void setRow(int rowIndex, float[] values) {
    DenseFloatVector ret = new DenseFloatVector((int)col, values);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    vectors[rowIndex] = ret;
  }

  /**
   * init the vector by set the value
   *
   * @param rowIndex
   * @param values
   * @return
   */
  private DenseFloatVector initVector(int rowIndex, float[] values) {
    DenseFloatVector ret = new DenseFloatVector((int)col, values);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }
}
