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
package com.tencent.angel.kubernetesmanager.scheduler

import java.io.Closeable

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.kubernetesmanager.deploy.config.Constants._
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watcher}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration

import scala.util.control.NonFatal

private[angel] class AngelExecutorPodsWatchSnapshotSource(
                                                           conf: Configuration,
                                                           snapshotsStore: AngelExecutorPodsSnapshotsStore,
                                                           kubernetesClient: KubernetesClient) {

  private final val LOG: Log = LogFactory.getLog(classOf[AngelExecutorPodsWatchSnapshotSource])

  private var watchConnection: Closeable = _

  def start(applicationId: String): Unit = {
    require(watchConnection == null, "Cannot start the watcher twice.")
    val executorRole = conf.get(AngelConf.ANGEL_KUBERNETES_EXECUTOR_ROLE,
      AngelConf.DEFAULT_ANGEL_KUBERNETES_EXECUTOR_ROLE)
    val executorRoleLabel = if (executorRole.equals("ps")) ANGEL_POD_PS_ROLE else ANGEL_POD_WORKER_ROLE
    LOG.debug(s"Starting watch for pods with labels $ANGEL_APP_ID_LABEL=$applicationId," +
      s" $ANGEL_ROLE_LABEL=$executorRoleLabel.")
    watchConnection = kubernetesClient.pods()
      .withLabel(ANGEL_APP_ID_LABEL, applicationId)
      .withLabel(ANGEL_ROLE_LABEL, executorRoleLabel)
      .watch(new ExecutorPodsWatcher())
  }

  def stop(): Unit = {
    if (watchConnection != null) {
      try {
        watchConnection.close()
      } catch {
        case NonFatal(t) =>
          LOG.error(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
      }
      watchConnection = null
    }
  }

  private class ExecutorPodsWatcher extends Watcher[Pod] {
    override def eventReceived(action: Action, pod: Pod): Unit = {
      val podName = pod.getMetadata.getName
      LOG.debug(s"Received executor pod update for pod named $podName, action $action")
      snapshotsStore.updatePod(pod)
    }

    override def onClose(e: KubernetesClientException): Unit = {
      LOG.warn("Kubernetes client has been closed (this is expected if the application is" +
        " shutting down.)", e)
    }
  }

}
