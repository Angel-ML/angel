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

package com.tencent.angel.psagent.matrix.transport;

import com.tencent.angel.ps.ParameterServerId;

/**
 * Refresh server address event
 */
public class RefreshServerLocationEvent extends DispatcherEvent {
  /**
   * server id
   */
  private final ParameterServerId serverId;

  /**
   * Is server location is updated
   */
  private final boolean isUpdated;

  /**
   * Create a new RefreshServerLocationEvent.
   *
   * @param type     event type
   * @param serverId server id
   */
  public RefreshServerLocationEvent(EventType type, ParameterServerId serverId) {
    this(type, serverId, false);
  }

  /**
   * Create a new RefreshServerLocationEvent.
   *
   * @param type      event type
   * @param serverId  server id
   * @param isUpdated is the server location is updated
   */
  public RefreshServerLocationEvent(EventType type, ParameterServerId serverId, boolean isUpdated) {
    super(type);
    this.serverId = serverId;
    this.isUpdated = isUpdated;
  }

  /**
   * Get server id.
   *
   * @return ParameterServerId  get server id
   */
  public ParameterServerId getServerId() {
    return serverId;
  }

  /**
   * Is that the server location updated
   *
   * @return true means update
   */
  public boolean isUpdated() {
    return isUpdated;
  }
}
