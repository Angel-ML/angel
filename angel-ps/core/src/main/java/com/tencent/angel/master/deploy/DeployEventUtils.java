package com.tencent.angel.master.deploy;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.master.deploy.embedded.EmbeddedContainerAllocatorEvent;
import com.tencent.angel.master.deploy.embedded.EmbeddedContainerLauncherEvent;
import com.tencent.angel.master.deploy.local.LocalContainerAllocatorEvent;
import com.tencent.angel.master.deploy.local.LocalContainerLauncherEvent;
import com.tencent.angel.master.deploy.yarn.ContainerRemoteLaunchEvent;
import com.tencent.angel.master.deploy.yarn.YarnContainerAllocatorEvent;
import com.tencent.angel.master.deploy.yarn.YarnContainerRequestEvent;
import com.tencent.angel.master.ps.attempt.PSAttempt;
import com.tencent.angel.master.yarn.util.ContainerContextUtils;
import com.tencent.angel.ps.PSAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

public class DeployEventUtils {

  public static ContainerAllocatorEvent createContainerAllocatorEvent(
      AngelDeployMode deployMode, PSAttempt psAttempt) {
    switch (deployMode) {
      case LOCAL:
      case STANDALONE: {
        return new LocalContainerAllocatorEvent(ContainerAllocatorEventType.CONTAINER_REQ,
            psAttempt.getId());
      }

      case EMBEDDED: {
        return new EmbeddedContainerAllocatorEvent(ContainerAllocatorEventType.CONTAINER_REQ,
            psAttempt.getId());
      }

      case YARN: {
        if (psAttempt.getExpectedIp() != null) {
          return new YarnContainerRequestEvent(psAttempt.getId(),
              psAttempt.getContext().getParameterServerManager().getPsResource(),
              psAttempt.getContext().getParameterServerManager().getPriority(),
              new String[]{psAttempt.getExpectedIp()});
        } else {
          return new YarnContainerRequestEvent(psAttempt.getId(),
              psAttempt.getContext().getParameterServerManager().getPsResource(),
              psAttempt.getContext().getParameterServerManager().getPriority());
        }
      }

      case KUBERNETES: {
        // TODO:
        throw new UnsupportedOperationException();
      }

      default: {
        throw new UnsupportedOperationException();
      }
    }
  }

  public static ContainerAllocatorEvent createContainerDeAllocatorEvent(
      AngelDeployMode deployMode, PSAttempt psAttempt) {
    switch (deployMode) {
      case LOCAL:
      case STANDALONE: {
        return
            new LocalContainerAllocatorEvent(ContainerAllocatorEventType.CONTAINER_DEALLOCATE,
                psAttempt.getId());
      }

      case EMBEDDED: {
        return new EmbeddedContainerAllocatorEvent(ContainerAllocatorEventType.CONTAINER_DEALLOCATE,
            psAttempt.getId());
      }

      case YARN: {
        return new YarnContainerAllocatorEvent(psAttempt.getId(),
            ContainerAllocatorEventType.CONTAINER_DEALLOCATE,
            psAttempt.getContext().getParameterServerManager().getPriority());
      }

      case KUBERNETES: {
        // TODO:
        throw new UnsupportedOperationException();
      }

      default: {
        throw new UnsupportedOperationException();
      }
    }
  }


  public static ContainerLauncherEvent createContainerLaunchEvent(
      AngelDeployMode deployMode, PSAttempt psAttempt) {
    switch (deployMode) {
      case LOCAL:
      case STANDALONE: {
        return
            new LocalContainerLauncherEvent(ContainerLauncherEventType.CONTAINER_REMOTE_LAUNCH,
                psAttempt.getId());
      }

      case EMBEDDED: {
        return new EmbeddedContainerLauncherEvent(
            ContainerLauncherEventType.CONTAINER_REMOTE_LAUNCH,
            psAttempt.getId());
      }

      case YARN: {
        PSAttemptId psAttemptId = psAttempt.getId();
        ContainerLaunchContext launchContext = ContainerContextUtils.createContainerLaunchContext(
            psAttempt.getContext().getContainerAllocator().getApplicationACLs(),
            psAttempt.getContext().getConf(),
            psAttemptId,
            psAttempt.getContext().getApplicationId(),
            psAttempt.getContext().getMasterService(),
            psAttempt.getContext().getCredentials(),
            psAttempt.getContainer().getId());

        return
            new ContainerRemoteLaunchEvent(psAttemptId, launchContext, psAttempt.getContainer());
      }

      default: {
        throw new UnsupportedOperationException();
      }
    }
  }
}
