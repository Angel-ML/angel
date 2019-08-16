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

/**
 * PS save result
 */
public class PSMatricesSaveResult {

  /**
   * Save request id
   */
  private volatile int requestId;

  /**
   * Save sub-request id
   */
  private volatile int subRequestId;

  /**
   * Save state
   */
  private volatile SaveState state;

  /**
   * Save detail failed message
   */
  private volatile String errorMsg;

  /**
   * Create a new PSMatricesSaveResult
   *
   * @param requestId request id
   * @param subRequestId sub-request id
   * @param state save state
   */
  public PSMatricesSaveResult(int requestId, int subRequestId, SaveState state) {
    this.requestId = requestId;
    this.subRequestId = subRequestId;
    this.state = state;
  }

  /**
   * Get request id
   *
   * @return request id
   */
  public int getRequestId() {
    return requestId;
  }

  /**
   * Get sub-request id
   *
   * @return sub-request id
   */
  public int getSubRequestId() {
    return subRequestId;
  }

  /**
   * Get save state
   *
   * @return save state
   */
  public SaveState getState() {
    return state;
  }

  /**
   * Set request id
   *
   * @param requestId request id
   */
  public void setRequestId(int requestId) {
    this.requestId = requestId;
  }

  /**
   * Set sub-request id
   *
   * @param subRequestId sub-request id
   */
  public void setSubRequestId(int subRequestId) {
    this.subRequestId = subRequestId;
  }

  /**
   * Set save state
   *
   * @param state save state
   */
  public void setState(SaveState state) {
    this.state = state;
  }

  /**
   * Get detail failed message
   *
   * @return detail failed message
   */
  public String getErrorMsg() {
    return errorMsg;
  }

  /**
   * Set detail failed message
   *
   * @param errorMsg detail failed message
   */
  public void setErrorMsg(String errorMsg) {
    this.errorMsg = errorMsg;
  }
}