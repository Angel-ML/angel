package com.tencent.angel.kubernetesmanager.scheduler

import io.fabric8.kubernetes.api.model.Pod

private[angel] trait AngelExecutorPodsSnapshotsStore {

  def addSubscriber
  (processBatchIntervalMillis: Long)
  (onNewSnapshots: Seq[AngelExecutorPodsSnapshot] => Unit)

  def stop(): Unit

  def updatePod(updatedPod: Pod): Unit

  def replaceSnapshot(newSnapshot: Seq[Pod]): Unit
}
