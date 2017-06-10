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

import com.tencent.angel.ml.math.vector.TFloatVector;
import com.tencent.angel.ml.math.TMatrix;
import com.tencent.angel.ml.math.TVector;

public abstract class TFloatMatrix extends TMatrix {

  /**
   * Create a new float matrix.
   *
   * @param row the row
   * @param col the col
   */
  public TFloatMatrix(int row, int col) {
    super(row, col);
  }

  /**
   * Increases specified element by value.
   *
   * @param rowId the row id
   * @param colId the col id
   * @param value the value
   */
  public abstract void inc(int rowId, int colId, float value);

  /**
   * Plus by other matrix.
   *
   * @param other the other
   */
  public abstract void plusBy(TMatrix other);

  /**
   * Plus by other vector
   *
   * @param other the other
   */
  public abstract void plusBy(TVector other);

  /**
   * Get specified value.
   *
   * @param rowId the row id
   * @param colId the col id
   * @return the value
   */
  public abstract float get(int rowId, int colId);

  /**
   * Gets specified vector.
   *
   * @param rowId the row id
   * @return the vector
   */
  public abstract TFloatVector getTFloatVector(int rowId);

  @Override
  public TVector getTVector(int rowId) {
    return getTFloatVector(rowId);
  }

}
