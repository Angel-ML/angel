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

package com.tencent.angel.master.deploy.local;

import com.tencent.angel.common.Id;
import com.tencent.angel.localcluster.LocalCluster;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.deploy.ContainerAllocator;
import com.tencent.angel.master.deploy.ContainerAllocatorEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptContainerAssignedEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptContainerAssignedEvent;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;

/**
 * Local container allocator.
 */
public class LocalContainerAllocator extends ContainerAllocator {
  /**master context*/
  private final AMContext context;
  
  /**
   * Create a LocalContainerAllocator
   * @param context master context
   */
  public LocalContainerAllocator(AMContext context) {
    super(LocalContainerAllocator.class.getName());
    this.context = context;
  }

  @Override
  public void handle(ContainerAllocatorEvent event) {
    switch (event.getType()) {
      case CONTAINER_REQ:
        requestContainer(event);
        break;

      case CONTAINER_DEALLOCATE:
        deallocateContainer(event);
        break;
        
      default:
        break;
    }
  }

  private void deallocateContainer(ContainerAllocatorEvent event) {

  }

  @SuppressWarnings("unchecked")
  private void requestContainer(ContainerAllocatorEvent event) {
    LocalContainer allocated = new LocalContainer();
    Id id = event.getTaskId();
    if (id instanceof PSAttemptId) {
      context.getEventHandler().handle(
          new PSAttemptContainerAssignedEvent((PSAttemptId) id, allocated));
    } else if (id instanceof WorkerAttemptId) {
      context.getEventHandler().handle(
          new WorkerAttemptContainerAssignedEvent((WorkerAttemptId) id, allocated));
    }
  }  
  
  @Override
  protected void serviceStop() throws Exception {
    if(!context.needClear()) {
      LocalCluster cluster = LocalClusterContext.get().getLocalCluster();
      if(cluster != null) {
        cluster.getLocalRM().masterExited(context.getApplicationId());
      }
    }
    
    super.serviceStop();
  }
}
