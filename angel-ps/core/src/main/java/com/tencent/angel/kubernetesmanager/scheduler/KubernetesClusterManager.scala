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

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.kubernetesmanager.deploy.utils.{AngelKubernetesClientFactory, ThreadUtils}
import com.tencent.angel.master.app.AMContext
import org.apache.hadoop.conf.Configuration

private[angel] class KubernetesClusterManager(context: AMContext) {

  private var psSchedulerBackend: KubernetesClusterSchedulerBackend = _
  private var workerSchedulerBackend: KubernetesClusterSchedulerBackend = _

  def doScheduler(conf: Configuration): Unit = {
    val (authConfPrefix,
    apiServerUri,
    defaultServiceAccountToken,
    defaultServiceAccountCaCrt) = (conf.get(AngelConf.ANGEL_KUBERNETES_KUBERNETES_AUTH_CLIENT_MODE_PREFIX),
      conf.get(AngelConf.ANGEL_KUBERNETES_MASTER),
      None,
      None)
    Option(conf.get(AngelConf.ANGEL_KUBERNETES_MASTER_POD_NAME))
      .getOrElse("The angel master pod name must be provided.")
    val executorRole = conf.get(AngelConf.ANGEL_KUBERNETES_EXECUTOR_ROLE,
      AngelConf.DEFAULT_ANGEL_KUBERNETES_EXECUTOR_ROLE)
    val namespace = conf.get(AngelConf.ANGEL_KUBERNETES_NAMESPACE, AngelConf.DEFAULT_ANGEL_KUBERNETES_NAMESPACE)
    val kubernetesClient = AngelKubernetesClientFactory.createKubernetesClient(
      apiServerUri,
      Some(namespace),
      authConfPrefix,
      conf,
      defaultServiceAccountToken,
      defaultServiceAccountCaCrt)
    val requestExecutorsService = ThreadUtils.newDaemonCachedThreadPool(
      "kubernetes-executor-requests" + "-" + executorRole)
    val subscribersExecutor = ThreadUtils
      .newDaemonThreadPoolScheduledExecutor(
        "kubernetes-executor-snapshots-subscribers" + "-" + executorRole, 2)
    val snapshotsStore = new AngelExecutorPodsSnapshotsStoreImpl(subscribersExecutor)
    val executorPodsLifecycleEventHandler = new AngelExecutorPodsLifecycleManager(
      conf,
      new KubernetesAngelExecutorBuilder(),
      kubernetesClient,
      snapshotsStore)
    val executorPodsAllocator = new AngelExecutorPodsAllocator(
      conf, new KubernetesAngelExecutorBuilder(), kubernetesClient, snapshotsStore)

    val podsWatchEventSource = new AngelExecutorPodsWatchSnapshotSource(
      conf,
      snapshotsStore,
      kubernetesClient)

    val eventsPollingExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "kubernetes-executor-pod-polling-sync" + "-" + executorRole)
    val podsPollingEventSource = new AngelExecutorPodsPollingSnapshotSource(
      conf, kubernetesClient, snapshotsStore, eventsPollingExecutor)

    if (psRole(conf)) {
      psSchedulerBackend = new KubernetesClusterSchedulerBackend(
        conf,
        kubernetesClient,
        requestExecutorsService,
        snapshotsStore,
        executorPodsAllocator,
        executorPodsLifecycleEventHandler,
        podsWatchEventSource,
        podsPollingEventSource)
    } else {
      workerSchedulerBackend = new KubernetesClusterSchedulerBackend(
        conf,
        kubernetesClient,
        requestExecutorsService,
        snapshotsStore,
        executorPodsAllocator,
        executorPodsLifecycleEventHandler,
        podsWatchEventSource,
        podsPollingEventSource)
    }
  }

  def scheduler(conf: Configuration): Unit = {
    doScheduler(conf)
    if (psRole(conf)) {
      psSchedulerBackend.start()
    } else {
      workerSchedulerBackend.start()
    }
  }

  def psRole(conf: Configuration): Boolean = {
    val executorRole = conf.get(AngelConf.ANGEL_KUBERNETES_EXECUTOR_ROLE,
      AngelConf.DEFAULT_ANGEL_KUBERNETES_EXECUTOR_ROLE)
    executorRole.equals("ps")
  }

  def stop(executorRole: String): Unit = {
    if (executorRole.equals("ps")) {
      psSchedulerBackend.stop()
    } else {
      workerSchedulerBackend.stop()
    }
  }
}

object KubernetesClusterManager {
  private var amContext: AMContext = _

  def apply(context: AMContext): KubernetesClusterManager = {
    amContext = context
    new KubernetesClusterManager(context)
  }

  def getContext(): AMContext = {
    amContext
  }
}
