package com.tencent.angel.master.deploy.embedded;

import com.tencent.angel.common.Id;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.deploy.ContainerAllocator;
import com.tencent.angel.master.deploy.ContainerAllocatorEvent;
import com.tencent.angel.master.deploy.local.LocalContainer;
import com.tencent.angel.master.ps.attempt.PSAttemptContainerAssignedEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptContainerAssignedEvent;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;

public class EmbeddedContainerAllocator extends ContainerAllocator {
  /**
   * master context
   */
  private final AMContext context;

  public EmbeddedContainerAllocator(AMContext context) {
    super(context.getApplicationName());
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

  @SuppressWarnings("unchecked") private void requestContainer(ContainerAllocatorEvent event) {
    LocalContainer allocated = new LocalContainer();
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
