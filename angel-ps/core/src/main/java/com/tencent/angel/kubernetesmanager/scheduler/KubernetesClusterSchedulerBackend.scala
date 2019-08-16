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

import java.util.concurrent.ExecutorService

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.kubernetesmanager.deploy.config.Constants
import com.tencent.angel.kubernetesmanager.deploy.config.Constants.{ANGEL_POD_PS_ROLE, ANGEL_POD_WORKER_ROLE}
import com.tencent.angel.kubernetesmanager.deploy.utils.ThreadUtils
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[angel] class KubernetesClusterSchedulerBackend(
                                                        conf: Configuration,
                                                        kubernetesClient: KubernetesClient,
                                                        requestExecutorsService: ExecutorService,
                                                        snapshotsStore: AngelExecutorPodsSnapshotsStore,
                                                        podAllocator: AngelExecutorPodsAllocator,
                                                        lifecycleEventHandler: AngelExecutorPodsLifecycleManager,
                                                        watchEvents: AngelExecutorPodsWatchSnapshotSource,
                                                        pollEvents: AngelExecutorPodsPollingSnapshotSource) {

  private final val LOG: Log = LogFactory.getLog(classOf[KubernetesClusterSchedulerBackend])

  private implicit val requestExecutorContext = ExecutionContext.fromExecutorService(
    requestExecutorsService)

  private val executorRole = conf.get(AngelConf.ANGEL_KUBERNETES_EXECUTOR_ROLE,
    AngelConf.DEFAULT_ANGEL_KUBERNETES_EXECUTOR_ROLE)

  private val initialExecutors = if (executorRole.equals("ps")) conf.getInt(AngelConf.ANGEL_PS_NUMBER,
    AngelConf.DEFAULT_ANGEL_PS_NUMBER) else conf.getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER,
    AngelConf.DEFAULT_ANGEL_WORKERGROUP_NUMBER)

  private val executorRoleLabel = if (executorRole.equals("ps")) ANGEL_POD_PS_ROLE else ANGEL_POD_WORKER_ROLE

  private val appId = s"angel-${conf.get(AngelConf.ANGEL_KUBERNETES_APP_ID).replaceAll("_", "-")}"

  def start(): Unit = {
    podAllocator.setTotalExpectedExecutors(initialExecutors)
    lifecycleEventHandler.start()
    podAllocator.start(appId)
    watchEvents.start(appId)
    pollEvents.start(appId)
  }

  def stop(): Unit = {

    tryLogNonFatalError(snapshotsStore.stop())

    tryLogNonFatalError(watchEvents.stop())

    tryLogNonFatalError(pollEvents.stop())

    tryLogNonFatalError {
      kubernetesClient.pods()
        .withLabel(Constants.ANGEL_APP_ID_LABEL, appId)
        .withLabel(Constants.ANGEL_ROLE_LABEL, executorRoleLabel)
        .delete()
    }

    tryLogNonFatalError(ThreadUtils.shutdown(requestExecutorsService))

    tryLogNonFatalError(kubernetesClient.close())
  }

  def tryLogNonFatalError(block: => Unit) {
    try {
      block
    } catch {
      case NonFatal(t) =>
        LOG.error(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
    }
  }
}
