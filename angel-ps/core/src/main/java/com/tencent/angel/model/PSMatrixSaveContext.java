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

import com.tencent.angel.model.io.IOExecutors;
import java.util.List;

/**
 * PS matrix save context
 */
public class PSMatrixSaveContext {

  /**
   * Matrix id
   */
  private final int matrixId;

  /**
   * Need save matrix partitions
   */
  private final List<Integer> partIds;

  /**
   * Need save matrix rows
   */
  private final List<Integer> rowIndexes;

  /**
   * Matrix output format class name
   */
  private final String formatClassName;

  /**
   * Matrix output files save path
   */
  private volatile String savePath;

  /**
   * Save workers
   */
  private volatile IOExecutors workers;

  /**
   * Clone the row before save
   */
  private final boolean cloneFirst;

  /**
   * Sort the row and col index first
   */
  private final boolean sortFirst;

  /**
   * Create a new PSMatrixSaveContext
   *
   * @param matrixId matrix id
   * @param partIds need save matrix partitions
   * @param rowIndexes need save matrix rows
   */
  public PSMatrixSaveContext(int matrixId, List<Integer> partIds, List<Integer> rowIndexes,
      String formatClassName, String savePath, boolean cloneFirst, boolean sortFirst) {
    this.matrixId = matrixId;
    this.partIds = partIds;
    this.rowIndexes = rowIndexes;
    this.formatClassName = formatClassName;
    this.savePath = savePath;
    this.cloneFirst = cloneFirst;
    this.sortFirst = sortFirst;
  }

  /**
   * Get matrix id
   *
   * @return matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Get need save matrix partitions
   *
   * @return need save matrix partitions
   */
  public List<Integer> getPartIds() {
    return partIds;
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
   * Get matrix output format class name
   *
   * @return matrix output format class name
   */
  public String getFormatClassName() {
    return formatClassName;
  }

  /**
   * Get matrix output files save path
   *
   * @return matrix output files save path
   */
  public String getSavePath() {
    return savePath;
  }

  /**
   * Set matrix output files save path
   *
   * @param savePath matrix output files save path
   */
  public void setSavePath(String savePath) {
    this.savePath = savePath;
  }

  /**
   * Get save workers
   *
   * @return save workers
   */
  public IOExecutors getWorkers() {
    return workers;
  }

  /**
   * Set save workers
   *
   * @param workers save workers
   */
  public void setWorkers(IOExecutors workers) {
    this.workers = workers;
  }

  /**
   * Is clone before save
   *
   * @return true means need clone first
   */
  public boolean cloneFirst() {
    return cloneFirst;
  }

  /**
   * Is sort the row or col index first
   *
   * @return true means sort first
   */
  public boolean sortFirst() {
    return sortFirst;
  }

}
