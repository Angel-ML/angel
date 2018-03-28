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
 *
 */

package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.vector.SparseLongKeyDoubleVector;
import com.tencent.angel.ml.math.vector.TLongDoubleVector;

/**
 * Base class of double matrix with long key row vector.
 */
public abstract class DoubleLongKeyMatrix<ROW extends  TLongDoubleVector> extends RowbaseMatrix<ROW> {
  /**
   * Create a LongKeyDoubleMatrix
   * @param row row number
   * @param col row vector dimension
   */
  public DoubleLongKeyMatrix(int row, long col,ROW[] vectors) {
    super(row, col, vectors);
  }



  /**
   * Plus specified element of matrix by a update value.
   *
   * @param rowIndex the row index
   * @param colIndex the column index
   * @param value the value update value
   * @return this
   */
  public DoubleLongKeyMatrix plusBy(int rowIndex, long colIndex, double value){
    if (vectors[rowIndex] == null) {
      vectors[rowIndex] = initVector(rowIndex);
    }
    vectors[rowIndex].plusBy(colIndex, value);
    return this;
  }

  /**
   * Increases specified elements by values.
   *
   * @param rowIndexes the row ids
   * @param colIndexes the col ids
   * @param values the values
   * @return this
   */
  public DoubleLongKeyMatrix plusBy(int[] rowIndexes, long[] colIndexes, double[] values) {
    assert ((rowIndexes.length == colIndexes.length) && (rowIndexes.length == values.length));
    for(int i = 0; i < rowIndexes.length; i++) {
      if(vectors[rowIndexes[i]] == null) {
        vectors[rowIndexes[i]] = initVector(rowIndexes[i]);
      }
      vectors[rowIndexes[i]].plusBy(colIndexes[i], values[i]);
    }
    return this;
  }

  /**
   * Increases specified row by values.
   *
   * @param rowIndex the row id
   * @param colIndexes the col ids
   * @param values the values
   * @return this
   */
  public DoubleLongKeyMatrix plusBy(int rowIndex, long[] colIndexes, double[] values) {
    assert (colIndexes.length == values.length);
    if(vectors[rowIndex] == null) {
      vectors[rowIndex] = initVector(rowIndex);
    }

    for(int i = 0; i < colIndexes.length; i++) {
      vectors[rowIndex].plusBy(colIndexes[i], values[i]);
    }
    return this;
  }

  /**
   * Get specified value.
   *
   * @param rowIndex the row index
   * @param colIndex the column index
   * @return the value
   */
  public double get(int rowIndex, long colIndex) {
    if(vectors[rowIndex] == null) {
      return 0.0;
    }
    return vectors[rowIndex].get(colIndex);
  }

}
