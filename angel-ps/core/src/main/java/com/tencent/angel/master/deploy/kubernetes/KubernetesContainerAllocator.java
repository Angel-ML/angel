package com.tencent.angel.master.deploy.kubernetes;

import com.tencent.angel.common.Id;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.kubernetesmanager.scheduler.KubernetesClusterManager;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.deploy.ContainerAllocator;
import com.tencent.angel.master.deploy.ContainerAllocatorEvent;
import com.tencent.angel.master.deploy.local.LocalContainer;
import com.tencent.angel.master.ps.attempt.PSAttemptContainerAssignedEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptContainerAssignedEvent;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;
import org.apache.hadoop.conf.Configuration;

public class KubernetesContainerAllocator extends ContainerAllocator {
  private final AMContext context;
  private KubernetesClusterManager clusterManager;

  public KubernetesContainerAllocator(AMContext context) {
    super(KubernetesContainerAllocator.class.getName());
    this.context = context;
  }

  @Override
  protected void serviceStart() throws Exception {
    clusterManager = KubernetesClusterManager.apply(context);
    Configuration psConf = new Configuration(context.getConf());
    psConf.set(AngelConf.ANGEL_KUBERNETES_EXECUTOR_ROLE, "ps");
    clusterManager.scheduler(psConf);
    if (context.getWorkerManager() != null) {
      Configuration workerConf = new Configuration(context.getConf());
      workerConf.set(AngelConf.ANGEL_KUBERNETES_EXECUTOR_ROLE, "worker");
      workerConf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER,
          workerConf.getInt(AngelConf.ANGEL_WORKERGROUP_ACTUAL_NUM,
              workerConf.getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER,
                  AngelConf.DEFAULT_ANGEL_WORKERGROUP_NUMBER)));
      clusterManager.scheduler(workerConf);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    try {
      if (clusterManager != null) {
        try {
          clusterManager.stop("worker");
        } finally {
          clusterManager.stop("ps");
        }
      }
    } finally {
      super.serviceStop();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void handle(ContainerAllocatorEvent event) {
    Id id = event.getTaskId();
    switch (event.getType()) {
      case CONTAINER_REQ:
        if (id instanceof PSAttemptId) {
          context.getEventHandler().handle(
              new PSAttemptContainerAssignedEvent((PSAttemptId) id, new LocalContainer()));
        } else if (id instanceof WorkerAttemptId) {
          context.getEventHandler().handle(
              new WorkerAttemptContainerAssignedEvent((WorkerAttemptId) id, new LocalContainer()));
        } else {
          throw new IllegalArgumentException("Unsupported Kubernetes attempt: " + id);
        }
        break;
      case CONTAINER_DEALLOCATE:
        removeAttempt(id);
        break;
      default:
        throw new IllegalArgumentException("Unsupported Kubernetes allocator event: " + event);
    }
  }

  public void removeAttempt(Id id) {
    if (id instanceof PSAttemptId) {
      PSAttemptId attempt = (PSAttemptId) id;
      context.getParameterServerManager().getPsAttemptIdBlockingQueue().remove(attempt);
      if (clusterManager != null) {
        clusterManager.deleteAttempt("ps", attempt.getPsId().getIndex(), attempt.getIndex());
      }
    } else if (id instanceof WorkerAttemptId) {
      WorkerAttemptId attempt = (WorkerAttemptId) id;
      context.getWorkerManager().getWorkerAttemptIdBlockingQueue().remove(attempt);
      if (clusterManager != null) {
        clusterManager.deleteAttempt("worker", attempt.getWorkerId().getWorkerGroupId().getIndex(),
            attempt.getIndex());
      }
    }
  }
}
