package com.tencent.angel.master.deploy.embedded;

import com.tencent.angel.common.Id;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.localcluster.LocalPS;
import com.tencent.angel.localcluster.LocalWorker;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.deploy.ContainerLauncher;
import com.tencent.angel.master.deploy.ContainerLauncherEvent;
import com.tencent.angel.master.deploy.local.LocalContainerLauncher;
import com.tencent.angel.master.ps.attempt.PSAttemptEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEventType;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEventType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class EmbeddedContainerLauncher extends ContainerLauncher {
  static final Log LOG = LogFactory.getLog(LocalContainerLauncher.class);
  /**
   * master context
   */
  private final AMContext context;

  public EmbeddedContainerLauncher(AMContext context) {
    super(context.getApplicationName());
    this.context = context;
  }

  @Override
  public void handle(ContainerLauncherEvent event) {
    switch (event.getType()) {
      case CONTAINER_REMOTE_LAUNCH:
        launch(event);
        break;

      case CONTAINER_REMOTE_CLEANUP:
        cleanup(event);
        break;

      default:
        break;
    }
  }

  @SuppressWarnings("unchecked") private void cleanup(ContainerLauncherEvent event) {
    Id id = event.getId();
    if (id instanceof PSAttemptId) {
      LocalPS ps = LocalClusterContext.get().getPS((PSAttemptId) id);
      if (ps != null) {
        ps.exit();
      }
    } else if (id instanceof WorkerAttemptId) {
      LocalWorker worker = LocalClusterContext.get().getWorker((WorkerAttemptId) id);
      if (worker != null) {
        worker.exit();
      }
    }
  }

  @SuppressWarnings("unchecked") private void launch(ContainerLauncherEvent event) {
    Id id = event.getId();
    if (id instanceof PSAttemptId) {
      context.getEventHandler()
          .handle(new PSAttemptEvent(PSAttemptEventType.PA_CONTAINER_LAUNCHED, (PSAttemptId) id));
    } else if (id instanceof WorkerAttemptId) {
      context.getEventHandler().handle(
          new WorkerAttemptEvent(WorkerAttemptEventType.CONTAINER_LAUNCHED, (WorkerAttemptId) id));
    }
  }
}
