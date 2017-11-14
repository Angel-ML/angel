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
import com.tencent.angel.master.deploy.ContainerLauncherEventType;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

/**
 * Yarn container launch event which CONTAINER_REMOTE_LAUNCH type
 */
public class ContainerRemoteLaunchEvent extends YarnContainerLauncherEvent {
  /**container need to launch*/
  private final Container allocatedContainer;
  
  /**container launch context, it contains remote process startup parameters*/
  private final ContainerLaunchContext containerLaunchContext;

  /**
   * Create a ContainerRemoteLaunchEvent
   * @param taskId task which the container is allocated to
   * @param containerLaunchContext container launch context
   * @param allocatedContainer container need to launch
   */
  public ContainerRemoteLaunchEvent(Id taskId, ContainerLaunchContext containerLaunchContext,
      Container allocatedContainer) {
    super(taskId, allocatedContainer.getId(), StringInterner.weakIntern(allocatedContainer
        .getNodeId().toString()), allocatedContainer.getContainerToken(),
        ContainerLauncherEventType.CONTAINER_REMOTE_LAUNCH);
    this.allocatedContainer = allocatedContainer;
    this.containerLaunchContext = containerLaunchContext;
  }

  /**
   * Get container launch context
   * @return container launch context
   */
  public ContainerLaunchContext getContainerLaunchContext() {
    return this.containerLaunchContext;
  }

  /**
   * Get container that need to launch
   * @return container that need to launch
   */
  public Container getAllocatedContainer() {
    return this.allocatedContainer;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }
}
