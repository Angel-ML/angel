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

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

import com.tencent.angel.common.Id;
import com.tencent.angel.master.deploy.ContainerAllocatorEventType;

/**
 * Request a container event. It contains resource context.
 */
public class YarnContainerRequestEvent extends YarnContainerAllocatorEvent {
  /**container resource quota*/
  private final Resource resource;
  
  /**the expected host addresses, it used to local calculation*/
  private final String[] hosts;
  
  /**
   * Create a YarnContainerRequestEvent
   * @param id task id
   * @param resouce container resource quota 
   * @param priority resource priority
   */
  public YarnContainerRequestEvent(Id id, Resource resouce,
      Priority priority) {
    this(id, resouce, priority, null);
  }

  /**
   * Create a YarnContainerRequestEvent
   * @param id task id
   * @param resouce container resource quota 
   * @param priority resource priority
   * @param hosts the expected host addresses
   */
  public YarnContainerRequestEvent(Id id, Resource resouce,
      Priority priority, String[] hosts) {
    super(id, ContainerAllocatorEventType.CONTAINER_REQ, priority);
    this.resource = resouce;
    this.hosts = hosts;
  }
  
  /**
   * Get container resource quota 
   * @return container resource quota 
   */
  public Resource getResource() {
    return resource;
  }

  /**
   * Get the expected host addresses
   * @return the expected host addresses
   */
  public String[] getHosts() {
    return hosts;
  }

  @Override
  public String toString() {
    return "YarnContainerRequestEvent [resource=" + resource + ", hosts=" + Arrays.toString(hosts) + ", toString()=" + super.toString() + "]";
  }
}
