/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.master.app;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Application failed event.
 */
public class InternalErrorEvent extends AppEvent {
  private static final Log LOG = LogFactory.getLog(InternalErrorEvent.class);
  
  /**details failed message*/
  private final String errorMsg;
  
  /**Is the application will retry?*/
  private final boolean shouldRetry;

  /**
   * Create a InternalErrorEvent
   * @param id application id
   * @param errorMsg detailed failed message
   */
  public InternalErrorEvent(ApplicationId id, String errorMsg) {
    this(id, errorMsg, false);
  }
  
  /**
   * Create a InternalErrorEvent
   * @param id application id
   * @param errorMsg detailed failed message
   * @param shouldRetry is the application will retry
   */
  public InternalErrorEvent(ApplicationId id, String errorMsg, boolean shouldRetry) {
    super(id, AppEventType.INTERNAL_ERROR);
    this.errorMsg = errorMsg;
    this.shouldRetry = shouldRetry;
    LOG.fatal(errorMsg);
  }

  /**
   * Get failed message
   * @return failed message
   */
  public String getErrorMsg() {
    return errorMsg;
  }

  @Override
  public String toString() {
    return "InternalErrorEvent [errorMsg=" + errorMsg + ", getType()=" + getType() + "]";
  }

  /**
   * Is the application will retry
   * @return true means retry later
   */
  public boolean isShouldRetry() {
    return shouldRetry;
  }
}
