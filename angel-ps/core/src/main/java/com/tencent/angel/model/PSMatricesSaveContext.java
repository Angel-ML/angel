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
 * PS matrices save context, it contains all need save matrices that are stored in this PS
 */
public class PSMatricesSaveContext {

  /**
   * Global save request id
   */
  private final int requestId;

  /**
   * Sub-save request id
   */
  private final int subRequestId;

  /**
   * Need save matrices
   */
  private final List<PSMatrixSaveContext> matrixSaveContexts;

  /**
   * Create a PSMatricesSaveContext
   *
   * @param requestId global save request id
   * @param subRequestId sub-save request id
   * @param matrixSaveContexts matrix save contexts
   */
  public PSMatricesSaveContext(int requestId, int subRequestId,
      List<PSMatrixSaveContext> matrixSaveContexts) {
    this.requestId = requestId;
    this.subRequestId = subRequestId;
    this.matrixSaveContexts = matrixSaveContexts;
  }

  /**
   * Get global save request id
   *
   * @return global save request id
   */
  public int getRequestId() {
    return requestId;
  }

  /**
   * Get Sub-save request id
   *
   * @return sub-save request id
   */
  public int getSubRequestId() {
    return subRequestId;
  }

  /**
   * Get matrix save contexts
   *
   * @return matrix save contexts
   */
  public List<PSMatrixSaveContext> getMatrixSaveContexts() {
    return matrixSaveContexts;
  }

}
