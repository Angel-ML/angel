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

import com.tencent.angel.common.Id;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * Base class of container launcher event.
 */
public abstract class ContainerLauncherEvent extends AbstractEvent<ContainerLauncherEventType> {
  /** the task the container is allocated to */
  private final Id id;

  /**
   * Create a container launch event
   * 
   * @param type event type
   * @param id task id
   */
  public ContainerLauncherEvent(ContainerLauncherEventType type, Id id) {
    super(type);
    this.id = id;
  }

  /**
   * Get task id
   * 
   * @return task id
   */
  public Id getId() {
    return id;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
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
    ContainerLauncherEvent other = (ContainerLauncherEvent) obj;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "ContainerLauncherEvent [id=" + id + "]";
  }

}
