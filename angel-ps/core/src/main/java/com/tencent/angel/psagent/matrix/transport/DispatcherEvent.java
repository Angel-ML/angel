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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.psagent.matrix.transport;


enum EventType {
  START_GET, GET_SUCCESS, GET_FAILED, GET_NOTREADY, START_PUT, PUT_SUCCESS, PUT_FAILED,
  ACTIVE_FAILED_TASK, END, PERIOD_CHECK, REFRESH_SERVER_LOCATION_SUCCESS,
  REFRESH_SERVER_LOCATION_FAILED, CHANNEL_CLOSED, SERVER_FAILED, SERVER_NORMAL, OOM
};

/**
 * PS RPC dispatch event.
 */
public class DispatcherEvent {
  /** event type*/
  private final EventType type;

  /**
   * Create a new DispatcherEvent.
   *
   * @param type event type
   */
  public DispatcherEvent(EventType type) {
    this.type = type;
  }

  /**
   * Get event type.
   * 
   * @return EventType event type
   */
  public EventType getType() {
    return type;
  }

  @Override
  public String toString() {
    return "DispatcherEvent [type=" + type + "]";
  }
}
