package com.tencent.angel.master.deploy.kubernetes;

import com.tencent.angel.common.Id;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.deploy.ContainerLauncher;
import com.tencent.angel.master.deploy.ContainerLauncherEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEventType;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEventType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;

public class KubernetesContainerLauncher extends ContainerLauncher {
  private final AMContext context;

  public KubernetesContainerLauncher(AMContext context) {
    super(KubernetesContainerLauncher.class.getName());
    this.context = context;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void handle(ContainerLauncherEvent event) {
    Id id = event.getId();
    switch (event.getType()) {
      case CONTAINER_REMOTE_LAUNCH:
        if (id instanceof PSAttemptId) {
          context.getEventHandler().handle(
              new PSAttemptEvent(PSAttemptEventType.PA_CONTAINER_LAUNCHED, (PSAttemptId) id));
        } else if (id instanceof WorkerAttemptId) {
          context.getEventHandler().handle(
              new WorkerAttemptEvent(WorkerAttemptEventType.CONTAINER_LAUNCHED,
                  (WorkerAttemptId) id));
        } else {
          throw new IllegalArgumentException("Unsupported Kubernetes attempt: " + id);
        }
        break;
      case CONTAINER_REMOTE_CLEANUP:
        ((KubernetesContainerAllocator) context.getContainerAllocator()).removeAttempt(id);
        break;
      default:
        throw new IllegalArgumentException("Unsupported Kubernetes launcher event: " + event);
    }
  }
}
