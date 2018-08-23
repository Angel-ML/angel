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
 * PS matrix load context
 */
public class PSMatrixLoadContext {
  /**
   * Matrix id
   */
  private final int matrixId;

  /**
   * Matrix save directory
   */
  private final String loadPath;

  /**
   * Need load matrix partitions
   */
  private final List<Integer> partIds;

  /**
   * Create a new PSMatrixLoadContext
   *
   * @param matrixId matrix id
   * @param loadPath matrix save directory
   * @param partIds  need load directory
   */
  public PSMatrixLoadContext(int matrixId, String loadPath, List<Integer> partIds) {
    this.matrixId = matrixId;
    this.loadPath = loadPath;
    this.partIds = partIds;
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
   * Get matrix save directory
   *
   * @return matrix save directory
   */
  public String getLoadPath() {
    return loadPath;
  }

  /**
   * Get need load partitons
   *
   * @return need load partitions
   */
  public List<Integer> getPartIds() {
    return partIds;
  }
}