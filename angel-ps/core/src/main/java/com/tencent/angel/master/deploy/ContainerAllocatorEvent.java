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

package com.tencent.angel.master.deploy;

import org.apache.hadoop.yarn.event.AbstractEvent;

import com.tencent.angel.common.Id;

/**
 * Base class for container allocator event.
 */
public abstract class ContainerAllocatorEvent extends AbstractEvent<ContainerAllocatorEventType> {
  /** the task that the container is allocated to or deallocated from */
  private final Id taskId;

  /**
   * Create a ContainerAllocatorEvent
   * 
   * @param type event type
   * @param taskId task id
   */
  public ContainerAllocatorEvent(ContainerAllocatorEventType type, Id taskId) {
    super(type);
    this.taskId = taskId;
  }

  /**
   * Get task id
   * 
   * @return task id
   */
  public Id getTaskId() {
    return taskId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContainerAllocatorEvent other = (ContainerAllocatorEvent) obj;
    if (taskId == null) {
      if (other.taskId != null)
        return false;
    } else if (!taskId.equals(other.taskId))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "ContainerAllocatorEvent [taskId=" + taskId + "]";
  }
}
