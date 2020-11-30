package com.tencent.angel.kubernetesmanager.scheduler

import com.tencent.angel.kubernetesmanager.deploy.config.Constants
import io.fabric8.kubernetes.api.model.Pod

/**
  * An immutable view of the current executor pods that are running in the cluster.
  */
private[angel] case class AngelExecutorPodsSnapshot(executorPods: Map[Long, ExecutorPodState]) {

  import AngelExecutorPodsSnapshot._

  def withUpdate(updatedPod: Pod): AngelExecutorPodsSnapshot = {
    val newExecutorPods = executorPods ++ toStatesByExecutorId(Seq(updatedPod))
    new AngelExecutorPodsSnapshot(newExecutorPods)
  }
}

object AngelExecutorPodsSnapshot {

  def apply(executorPods: Seq[Pod]): AngelExecutorPodsSnapshot = {
    AngelExecutorPodsSnapshot(toStatesByExecutorId(executorPods))
  }

  def apply(): AngelExecutorPodsSnapshot = AngelExecutorPodsSnapshot(Map.empty[Long, ExecutorPodState])

  private def toStatesByExecutorId(executorPods: Seq[Pod]): Map[Long, ExecutorPodState] = {
    executorPods.map { pod =>
      (pod.getMetadata.getLabels.get(Constants.ANGEL_EXECUTOR_ID_LABEL).toLong, toState(pod))
    }.toMap
  }

  private def toState(pod: Pod): ExecutorPodState = {
    if (isDeleted(pod)) {
      PodDeleted(pod)
    } else {
      val phase = pod.getStatus.getPhase.toLowerCase
      phase match {
        case "pending" =>
          PodPending(pod)
        case "running" =>
          PodRunning(pod)
        case "failed" =>
          PodFailed(pod)
        case "succeeded" =>
          PodSucceeded(pod)
        case _ =>
          println(s"Received unknown phase $phase for executor pod with name" +
            s" ${pod.getMetadata.getName} in namespace ${pod.getMetadata.getNamespace}")
          PodUnknown(pod)
      }
    }
  }

  private def isDeleted(pod: Pod): Boolean = pod.getMetadata.getDeletionTimestamp != null
}
