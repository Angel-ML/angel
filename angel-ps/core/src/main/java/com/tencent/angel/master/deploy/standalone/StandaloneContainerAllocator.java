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

package com.tencent.angel.master.deploy.standalone;

import com.tencent.angel.common.Id;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.deploy.ContainerAllocator;
import com.tencent.angel.master.deploy.ContainerAllocatorEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptContainerAssignedEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptContainerAssignedEvent;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;

/**
 * Container allocator for {@link com.tencent.angel.AngelDeployMode#STANDALONE}. There is no real
 * resource scheduler; every request is satisfied immediately with a placeholder container.
 */
public class StandaloneContainerAllocator extends ContainerAllocator {

  private final AMContext context;

  public StandaloneContainerAllocator(AMContext context) {
    super(StandaloneContainerAllocator.class.getName());
    this.context = context;
  }

  @Override public void handle(ContainerAllocatorEvent event) {
    switch (event.getType()) {
      case CONTAINER_REQ:
        requestContainer(event);
        break;

      case CONTAINER_DEALLOCATE:
        // nothing to do, process lifecycle is handled by launcher
        break;

      default:
        break;
    }
  }

  @SuppressWarnings("unchecked") private void requestContainer(ContainerAllocatorEvent event) {
    StandaloneContainer allocated = new StandaloneContainer();
    Id id = event.getTaskId();
    if (id instanceof PSAttemptId) {
      context.getEventHandler()
          .handle(new PSAttemptContainerAssignedEvent((PSAttemptId) id, allocated));
    } else if (id instanceof WorkerAttemptId) {
      context.getEventHandler()
          .handle(new WorkerAttemptContainerAssignedEvent((WorkerAttemptId) id, allocated));
    }
  }
}
