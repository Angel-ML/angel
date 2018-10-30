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
 * PS matrices load context, it contains all need load matrices partitions stored in the PS
 */
public class PSMatricesLoadContext {
  /**
   * Global load request id
   */
  private final int requestId;

  /**
   * Sub-load request id
   */
  private final int subRequestId;

  /**
   * Need load matrices
   */
  private final List<PSMatrixLoadContext> matrixLoadContexts;

  /**
   * Create a PSMatricesLoadContext
   *
   * @param requestId          global load request id
   * @param subRequestId       sub-load request id
   * @param matrixLoadContexts matrix load contexts
   */
  public PSMatricesLoadContext(int requestId, int subRequestId,
    List<PSMatrixLoadContext> matrixLoadContexts) {
    this.requestId = requestId;
    this.subRequestId = subRequestId;
    this.matrixLoadContexts = matrixLoadContexts;
  }

  /**
   * Get global load request id
   *
   * @return global load request id
   */
  public int getRequestId() {
    return requestId;
  }

  /**
   * Get Sub-load request id
   *
   * @return sub-load request id
   */
  public int getSubRequestId() {
    return subRequestId;
  }

  /**
   * Get matrix load contexts
   *
   * @return matrix load contexts
   */
  public List<PSMatrixLoadContext> getMatrixLoadContexts() {
    return matrixLoadContexts;
  }
}
