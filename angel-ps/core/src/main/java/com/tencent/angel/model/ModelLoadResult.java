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
 * Model load result
 */
public class ModelLoadResult {
  /**
   * Load request id
   */
  private final int requestId;

  /**
   * Load state
   */
  private volatile LoadState state;

  /**
   * Detail failed message
   */
  private volatile String message;

  /**
   * Create a new ModelLoadResult
   *
   * @param requestId request id
   */
  public ModelLoadResult(int requestId) {
    this.requestId = requestId;
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
   * Get load state
   *
   * @return load state
   */
  public LoadState getState() {
    return state;
  }

  /**
   * Set load state
   *
   * @param state load state
   */
  public void setState(LoadState state) {
    this.state = state;
  }

  /**
   * Get detail failed message
   *
   * @return detail failed message
   */
  public String getMessage() {
    return message;
  }

  /**
   * Set detail failed message
   *
   * @param message detail failed message
   */
  public void setMessage(String message) {
    this.message = message;
  }
}