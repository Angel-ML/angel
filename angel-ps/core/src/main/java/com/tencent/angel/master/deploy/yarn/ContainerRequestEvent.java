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
import com.tencent.angel.master.deploy.ContainerAllocatorEventType;

import org.apache.hadoop.yarn.api.records.Resource;

public class ContainerRequestEvent extends YarnContainerAllocatorEvent {

  private final Resource capability;
  private final String[] hosts;
  private final String[] racks;

  public ContainerRequestEvent(Id id, Resource capability, String[] hosts, String[] racks) {
    super(id, ContainerAllocatorEventType.CONTAINER_REQ);
    this.capability = capability;
    this.hosts = hosts;
    this.racks = racks;
  }

  ContainerRequestEvent(Id id, Resource capability) {
    this(id, capability, new String[0], new String[0]);
  }

  public static ContainerRequestEvent createContainerRequestEventForFailedContainer(Id id,
      Resource capability) {
    // ContainerRequest for failed events does not consider rack / node locality?
    return new ContainerRequestEvent(id, capability);
  }

  public Resource getCapability() {
    return capability;
  }

  public String[] getHosts() {
    return hosts;
  }

  public String[] getRacks() {
    return racks;
  }
}
