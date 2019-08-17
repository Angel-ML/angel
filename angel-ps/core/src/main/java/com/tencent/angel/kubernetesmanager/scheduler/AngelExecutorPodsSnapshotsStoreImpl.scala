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

import java.util.concurrent._

import com.tencent.angel.kubernetesmanager.deploy.utils.ThreadUtils
import io.fabric8.kubernetes.api.model.Pod
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

private[angel] class AngelExecutorPodsSnapshotsStoreImpl(subscribersExecutor: ScheduledExecutorService)
  extends AngelExecutorPodsSnapshotsStore {

  private val SNAPSHOT_LOCK = new Object()

  private val subscribers = mutable.Buffer.empty[SnapshotsSubscriber]
  private val pollingTasks = mutable.Buffer.empty[Future[_]]

  @GuardedBy("SNAPSHOT_LOCK")
  private var currentSnapshot = AngelExecutorPodsSnapshot()

  override def addSubscriber(
                              processBatchIntervalMillis: Long)
                            (onNewSnapshots: Seq[AngelExecutorPodsSnapshot] => Unit): Unit = {
    val newSubscriber = SnapshotsSubscriber(
      new LinkedBlockingQueue[AngelExecutorPodsSnapshot](), onNewSnapshots)
    SNAPSHOT_LOCK.synchronized {
      newSubscriber.snapshotsBuffer.add(currentSnapshot)
    }
    subscribers += newSubscriber
    pollingTasks += subscribersExecutor.scheduleWithFixedDelay(
      toRunnable(() => callSubscriber(newSubscriber)),
      0L,
      processBatchIntervalMillis,
      TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = {
    pollingTasks.foreach(_.cancel(true))
    ThreadUtils.shutdown(subscribersExecutor)
  }

  override def updatePod(updatedPod: Pod): Unit = SNAPSHOT_LOCK.synchronized {
    currentSnapshot = currentSnapshot.withUpdate(updatedPod)
    addCurrentSnapshotToSubscribers()
  }

  override def replaceSnapshot(newSnapshot: Seq[Pod]): Unit = SNAPSHOT_LOCK.synchronized {
    currentSnapshot = AngelExecutorPodsSnapshot(newSnapshot)
    addCurrentSnapshotToSubscribers()
  }

  private def addCurrentSnapshotToSubscribers(): Unit = {
    subscribers.foreach { subscriber =>
      subscriber.snapshotsBuffer.add(currentSnapshot)
    }
  }

  private def callSubscriber(subscriber: SnapshotsSubscriber): Unit = {
    try {
      val currentSnapshots = mutable.Buffer.empty[AngelExecutorPodsSnapshot].asJava
      subscriber.snapshotsBuffer.drainTo(currentSnapshots)
      subscriber.onNewSnapshots(currentSnapshots.asScala)
    } catch {
      case NonFatal(t) =>
        println(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
    }
  }

  private def toRunnable[T](runnable: () => Unit): Runnable = new Runnable {
    override def run(): Unit = runnable()
  }

  private case class SnapshotsSubscriber(
                                          snapshotsBuffer: BlockingQueue[AngelExecutorPodsSnapshot],
                                          onNewSnapshots: Seq[AngelExecutorPodsSnapshot] => Unit)

}
