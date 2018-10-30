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


package com.tencent.angel.model;

import com.tencent.angel.model.output.format.RowIdColIdValueTextRowFormat;

import java.util.ArrayList;
import java.util.List;

/**
 * Matrix save context
 */
public class MatrixSaveContext {
  /**
   * Matrix name
   */
  private final String matrixName;

  /**
   * Matrix output format class name
   */
  private final String formatClassName;

  /**
   * Need save matrix rows
   */
  private final List<Integer> rowIndexes;

  private static final String defaultFormatClass = RowIdColIdValueTextRowFormat.class.getName();

  /**
   * Create a new MatrixSaveContext
   *
   * @param matrixName matrix name
   */
  public MatrixSaveContext(String matrixName, String formatClassName) {
    this(matrixName, new ArrayList<>(), formatClassName);
  }

  /**
   * Create a new MatrixSaveContext
   *
   * @param matrixName matrix name
   */
  public MatrixSaveContext(String matrixName) {
    this(matrixName, new ArrayList<>(), defaultFormatClass);
  }


  /**
   * Create a new MatrixSaveContext
   *
   * @param matrixName matrix name
   * @param rowIndexes need save matrix rows
   * @param formatClassName matrix save format class name
   */
  public MatrixSaveContext(String matrixName, List<Integer> rowIndexes, String formatClassName) {
    this.matrixName = matrixName;
    this.rowIndexes = rowIndexes;
    this.formatClassName = formatClassName;
  }

  public void addIndex(int idx) {
    this.rowIndexes.add(idx);
  }

  public void addIndices(int[] indices) {
    for (int idx : indices) {
      this.rowIndexes.add(idx);
    }
  }

  /**
   * Get matrix name
   *
   * @return matrix name
   */
  public String getMatrixName() {
    return matrixName;
  }

  /**
   * Get need save matrix rows
   *
   * @return need save matrix rows
   */
  public List<Integer> getRowIndexes() {
    return rowIndexes;
  }

  /**
   * Get matrix save format class
   * @return matrix save format class
   */
  public String getFormatClassName() {
    return formatClassName;
  }
}
