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
package com.tencent.angel.kubernetesmanager.deploy.submit

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.tencent.angel.kubernetesmanager.deploy.utils.ThreadUtils
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.{KubernetesClientException, Watcher}
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.JavaConverters._

private[angel] trait LoggingPodStatusWatcher extends Watcher[Pod] {
  def awaitCompletion(): Unit
}

/**
  * A monitor for the running Kubernetes pod of a Angel application. Status logging occurs on
  * every state change and also at an interval for liveness.
  *
  * @param appId                application ID.
  * @param maybeLoggingInterval ms between each state request. If provided, must be a positive
  *                             number.
  */
private[angel] class LoggingPodStatusWatcherImpl(
                                                  appId: String,
                                                  maybeLoggingInterval: Option[Long])
  extends LoggingPodStatusWatcher {

  private final val LOG: Log = LogFactory.getLog(classOf[LoggingPodStatusWatcherImpl])

  private val podCompletedFuture = new CountDownLatch(1)
  // start timer for periodic logging
  private val scheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("logging-pod-status-watcher")
  private val logRunnable: Runnable = new Runnable {
    override def run() = logShortStatus()
  }

  private var pod = Option.empty[Pod]

  private def phase: String = pod.map(_.getStatus.getPhase).getOrElse("unknown")

  def start(): Unit = {
    maybeLoggingInterval.foreach { interval =>
      scheduler.scheduleAtFixedRate(logRunnable, 0, interval, TimeUnit.MILLISECONDS)
    }
  }

  override def eventReceived(action: Action, pod: Pod): Unit = {
    this.pod = Option(pod)
    action match {
      case Action.DELETED | Action.ERROR =>
        closeWatch()

      case _ =>
        logLongStatus()
        if (hasCompleted()) {
          closeWatch()
        }
    }
  }

  override def onClose(e: KubernetesClientException): Unit = {
    LOG.debug(s"Stopping watching application $appId with last-observed phase $phase")
    closeWatch()
  }

  private def logShortStatus() = {
    LOG.info(s"Application status for $appId (phase: $phase)")
  }

  private def logLongStatus() = {
    LOG.info("State changed, new state: " + pod.map(formatPodState).getOrElse("unknown"))
  }

  private def hasCompleted(): Boolean = {
    phase == "Succeeded" || phase == "Failed"
  }

  private def closeWatch(): Unit = {
    podCompletedFuture.countDown()
    scheduler.shutdown()
  }

  private def formatPodState(pod: Pod): String = {
    val details = Seq[(String, String)](
      // pod metadata
      ("pod name", pod.getMetadata.getName),
      ("namespace", pod.getMetadata.getNamespace),
      ("labels", pod.getMetadata.getLabels.asScala.mkString(", ")),
      ("pod uid", pod.getMetadata.getUid),
      ("creation time", formatTime(pod.getMetadata.getCreationTimestamp)),

      // spec details
      ("service account name", pod.getSpec.getServiceAccountName),
      ("volumes", pod.getSpec.getVolumes.asScala.map(_.getName).mkString(", ")),
      ("node name", pod.getSpec.getNodeName),

      // status
      ("start time", formatTime(pod.getStatus.getStartTime)),
      ("container images",
        pod.getStatus.getContainerStatuses
          .asScala
          .map(_.getImage)
          .mkString(", ")),
      ("phase", pod.getStatus.getPhase),
      ("status", pod.getStatus.getContainerStatuses.toString)
    )

    formatPairsBundle(details)
  }

  private def formatPairsBundle(pairs: Seq[(String, String)]) = {
    // Use more loggable format if value is null or empty
    pairs.map {
      case (k, v) => s"\n\t $k: ${Option(v).filter(_.nonEmpty).getOrElse("N/A")}"
    }.mkString("")
  }

  override def awaitCompletion(): Unit = {
    podCompletedFuture.await()
    println(pod.map { p =>
      s"info: Container final statuses:\n\n${containersDescription(p)}"
    }.getOrElse("No containers were found in the angel master pod."))
  }

  private def containersDescription(p: Pod): String = {
    p.getStatus.getContainerStatuses.asScala.map { status =>
      Seq(
        ("Container name", status.getName),
        ("Container image", status.getImage)) ++
        containerStatusDescription(status)
    }.map(formatPairsBundle).mkString("\n\n")
  }

  private def containerStatusDescription(
                                          containerStatus: ContainerStatus): Seq[(String, String)] = {
    val state = containerStatus.getState
    Option(state.getRunning)
      .orElse(Option(state.getTerminated))
      .orElse(Option(state.getWaiting))
      .map {
        case running: ContainerStateRunning =>
          Seq(
            ("Container state", "Running"),
            ("Container started at", formatTime(running.getStartedAt)))
        case waiting: ContainerStateWaiting =>
          Seq(
            ("Container state", "Waiting"),
            ("Pending reason", waiting.getReason))
        case terminated: ContainerStateTerminated =>
          Seq(
            ("Container state", "Terminated"),
            ("Exit code", terminated.getExitCode.toString))
        case unknown =>
          throw new Exception(s"Unexpected container status type ${unknown.getClass}.")
      }.getOrElse(Seq(("Container state", "N/A")))
  }

  private def formatTime(time: Time): String = {
    if (time != null) time.getTime else "N/A"
  }
}
