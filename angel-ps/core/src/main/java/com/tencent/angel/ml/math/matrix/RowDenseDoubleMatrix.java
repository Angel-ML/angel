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

import com.tencent.angel.ml.math.vector.TDoubleVector;


public class RowDenseDoubleMatrix extends TDoubleMatrix {

  public RowDenseDoubleMatrix(int row, int col) {
    super(row, col);
  }

  @Override
  public void inc(int rowId, int colId, double value) {

  }

  @Override
  public void plusBy(TDoubleMatrix other) {

  }

  @Override
  public void plusBy(TDoubleVector other) {

  }

  @Override
  public double get(int rowId, int colId) {
    return 0;
  }

  @Override
  public TDoubleVector getTDoubleVector(int rowId) {
    return null;
  }

  @Override
  public double sparsity() {
    return 0;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public void clear() {

  }

  @Override
  public int nonZeroNum() {
    return 0;
  }
}
