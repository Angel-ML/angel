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

package com.tencent.angel.master.deploy.yarn;

import com.tencent.angel.common.Id;
import com.tencent.angel.master.deploy.ContainerLauncherEvent;
import com.tencent.angel.master.deploy.ContainerLauncherEventType;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Token;

public class YarnContainerLauncherEvent extends ContainerLauncherEvent {
  private ContainerId containerId;
  private String containerMgrAddress;
  private Token containerToken;

  public YarnContainerLauncherEvent(Id id, ContainerId containerId, String containerMgrAddress,
      Token containerToken, ContainerLauncherEventType type) {
    super(type, id);
    this.containerId = containerId;
    this.containerMgrAddress = containerMgrAddress;
    this.containerToken = containerToken;
  }
  
  public ContainerId getContainerId() {
    return containerId;
  }

  public String getContainerMgrAddress() {
    return containerMgrAddress;
  }

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
