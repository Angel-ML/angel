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
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.localcluster.LocalPS;
import com.tencent.angel.localcluster.LocalWorker;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.deploy.ContainerLauncher;
import com.tencent.angel.master.deploy.ContainerLauncherEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEventType;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEventType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Local container launcher.
 */
public class LocalContainerLauncher extends ContainerLauncher {
  static final Log LOG = LogFactory.getLog(LocalContainerLauncher.class);
  /**master context*/
  private final AMContext context;

  /**
   * Create a LocalContainerLauncher
   * @param context master context
   */
  public LocalContainerLauncher(AMContext context) {
    super("local-container-launcher");
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

  @SuppressWarnings("unchecked")
  private void cleanup(ContainerLauncherEvent event) {
    Id id = event.getId();
    if (id instanceof PSAttemptId) {
      LocalPS ps = LocalClusterContext.get().getPS((PSAttemptId) id);
      if(ps != null) {
        ps.exit();
      }
    } else if (id instanceof WorkerAttemptId) {
      LocalWorker worker = LocalClusterContext.get().getWorker((WorkerAttemptId) id);
      if(worker != null) {
        worker.exit();
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void launch(ContainerLauncherEvent event) {
    Id id = event.getId();
    if (id instanceof PSAttemptId) {
      LocalPS ps = new LocalPS((PSAttemptId) id, context.getMasterService().getLocation(), context.getConf());
      context.getEventHandler().handle(
          new PSAttemptEvent(PSAttemptEventType.PA_CONTAINER_LAUNCHED, (PSAttemptId) id));
      
      try {
        ps.start();
        LocalClusterContext.get().addPS((PSAttemptId) id, ps);
      } catch (Exception e) {
        LOG.error("launch ps failed.", e);
        context.getEventHandler().handle(
            new PSAttemptEvent(PSAttemptEventType.PA_CONTAINER_LAUNCH_FAILED, (PSAttemptId) id));
      }
    } else if (id instanceof WorkerAttemptId) {
      LocalWorker worker = new LocalWorker(context.getConf(), 
          context.getApplicationId(), context.getUser(), (WorkerAttemptId) id, 
          context.getMasterService().getLocation(), 0, false);
      
      context.getEventHandler().handle(
          new WorkerAttemptEvent(WorkerAttemptEventType.CONTAINER_LAUNCHED, (WorkerAttemptId) id));
      
      try {
        worker.start();
        LocalClusterContext.get().addWorker((WorkerAttemptId) id, worker);
      } catch (Exception e) {
        LOG.error("launch worker failed.", e);
        context.getEventHandler().handle(
            new WorkerAttemptEvent(WorkerAttemptEventType.CONTAINER_LAUNCH_FAILED, (WorkerAttemptId) id));
      }
    }
  } 
}
