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

import com.tencent.angel.ml.math.vector.CompSparseIntVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Sparse double matrix that is represented by a group of component sparse int vector  {@link CompSparseIntVector}
 */
public class CompSparseIntMatrix extends TIntMatrix<CompSparseIntVector> {
  private static final Log LOG = LogFactory.getLog(SparseIntMatrix.class);

  /**
   * Create a ComponentSparseIntMatrix
   *
   * @param row the row number
   * @param col the col number
   */
  public CompSparseIntMatrix(int row, long col) {
    this(row, col, new CompSparseIntVector[row]);
  }

  public CompSparseIntMatrix(int row, long col, CompSparseIntVector[] vectors) {
    super(row, col, vectors);
  }

  /**
   * init the empty vector
   *
   * @param rowIndex row index
   * @return
   */
  @Override public CompSparseIntVector initVector(int rowIndex) {
    return new CompSparseIntVector(matrixId, rowIndex, (int)col);
  }
}
