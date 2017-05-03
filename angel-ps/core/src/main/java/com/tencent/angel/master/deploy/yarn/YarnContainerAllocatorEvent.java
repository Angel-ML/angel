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

import java.util.Arrays;

import com.tencent.angel.common.Id;
import com.tencent.angel.master.deploy.ContainerAllocatorEvent;
import com.tencent.angel.master.deploy.ContainerAllocatorEventType;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

public class YarnContainerAllocatorEvent extends ContainerAllocatorEvent {
  private final Resource resource;
  private final Priority priority;
  private final String[] hosts;

  public YarnContainerAllocatorEvent(ContainerAllocatorEventType type) {
    this(null, type, null, null, null);
  }

  public YarnContainerAllocatorEvent(Id id, ContainerAllocatorEventType type) {
    this(id, type, null, null, null);
  }

  public YarnContainerAllocatorEvent(Id id, ContainerAllocatorEventType type, Resource resouce,
      Priority priority) {
    this(id, type, resouce, priority, null);
  }

  public YarnContainerAllocatorEvent(Id id, ContainerAllocatorEventType type, Resource resouce,
      Priority priority, String[] hosts) {
    super(type, id);
    this.resource = resouce;
    this.priority = priority;
    this.hosts = hosts;
  }

  public Resource getResource() {
    return resource;
  }

  public Priority getPriority() {
    return priority;
  }

  public String[] getHosts() {
    return hosts;
  }

  @Override
  public String toString() {
    return "ContainerAllocatorEvent [resource=" + resource + ", priority="
        + priority + ", hosts=" + Arrays.toString(hosts) + ", toString()=" + super.toString() + "]";
  }
}
