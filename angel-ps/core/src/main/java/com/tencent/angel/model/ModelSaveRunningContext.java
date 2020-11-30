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
 * Model save running context
 */
public class ModelSaveRunningContext {
  /**
   * Save request id
   */
  private final int requestId;

  /**
   * Save trigger mode
   */
  private final SaveTriggerMode triggerMode;

  /**
   * Save context
   */
  private final ModelSaveContext saveContext;

  /**
   * Create a ModelSaveRunningContext
   *
   * @param requestId   Save request id
   * @param triggerMode Save trigger mode
   * @param saveContext Save context
   */
  public ModelSaveRunningContext(int requestId, SaveTriggerMode triggerMode,
    ModelSaveContext saveContext) {
    this.requestId = requestId;
    this.triggerMode = triggerMode;
    this.saveContext = saveContext;
  }

  /**
   * Get save request id
   *
   * @return save request id
   */
  public int getRequestId() {
    return requestId;
  }

  /**
   * Get save trigger mode
   *
   * @return save trigger mode
   */
  public SaveTriggerMode getTriggerMode() {
    return triggerMode;
  }

  /**
   * Get save context
   *
   * @return save context
   */
  public ModelSaveContext getSaveContext() {
    return saveContext;
  }
}
