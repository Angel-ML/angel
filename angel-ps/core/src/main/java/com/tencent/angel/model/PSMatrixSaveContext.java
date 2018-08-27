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
   * Create a new PSMatrixSaveContext
   *
   * @param matrixId   matrix id
   * @param partIds    need save matrix partitions
   * @param rowIndexes need save matrix rows
   */
  public PSMatrixSaveContext(int matrixId, List<Integer> partIds, List<Integer> rowIndexes) {
    this.matrixId = matrixId;
    this.partIds = partIds;
    this.rowIndexes = rowIndexes;
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
}