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

package com.tencent.angel.master.deploy.yarn;

import com.tencent.angel.common.Id;
import com.tencent.angel.master.deploy.ContainerLauncherEvent;
import com.tencent.angel.master.deploy.ContainerLauncherEventType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Token;

/**
 * Base class of Yarn container launch event.
 */
public class YarnContainerLauncherEvent extends ContainerLauncherEvent {
  /**container id*/
  private ContainerId containerId;
  
  /**Yarn nodemanager address*/
  private String containerMgrAddress;
  
  /**token between master and the Yarn nodemanager*/
  private Token containerToken;

  /**
   * Create a YarnContainerLauncherEvent
   * @param id task which the container is allocated to
   * @param containerId container id
   * @param containerMgrAddress Yarn nodemanager address
   * @param containerToken token between master and the Yarn nodemanager
   * @param type event type
   */
  public YarnContainerLauncherEvent(Id id, ContainerId containerId, String containerMgrAddress,
      Token containerToken, ContainerLauncherEventType type) {
    super(type, id);
    this.containerId = containerId;
    this.containerMgrAddress = containerMgrAddress;
    this.containerToken = containerToken;
  }
  
  /**
   * Get container id
   * @return container id
   */
  public ContainerId getContainerId() {
    return containerId;
  }

  /**
   * Get Yarn nodemanager address
   * @return Yarn nodemanager address
   */
  public String getContainerMgrAddress() {
    return containerMgrAddress;
  }

  /**
   * Get token between master and the Yarn nodemanager
   * @return token between master and the Yarn nodemanager
   */
  public Token getContainerToken() {
    return containerToken;
  }

  @Override
  public String toString() {
    return "YarnContainerLauncherEvent [containerId=" + containerId + ", containerMgrAddress="
        + containerMgrAddress + ", containerToken=" + containerToken + ", toString()="
        + super.toString() + "]";
  }
}
