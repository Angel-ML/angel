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

package com.tencent.angel.master.deploy.yarn;

import com.tencent.angel.common.Id;
import com.tencent.angel.master.deploy.ContainerAllocatorEvent;
import com.tencent.angel.master.deploy.ContainerAllocatorEventType;
import org.apache.hadoop.yarn.api.records.Priority;

/**
 * Yarn container allocator event.
 */
public class YarnContainerAllocatorEvent extends ContainerAllocatorEvent {
  /**resource priority*/
  private final Priority priority;
  
  /**
   * Create a YarnContainerAllocatorEvent
   * @param id task id
   * @param type event type
   */
  public YarnContainerAllocatorEvent(Id id, ContainerAllocatorEventType type, Priority priority) {
    super(type, id);
    this.priority = priority;
  }
  
  /**
   * Get resource priority
   * @return resource priority
   */
  public Priority getPriority() {
    return priority;
  }
}
