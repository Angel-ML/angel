package com.tencent.angel.kubernetesmanager.scheduler

import io.fabric8.kubernetes.api.model.Pod

sealed trait ExecutorPodState {
  def pod: Pod
}

case class PodRunning(pod: Pod) extends ExecutorPodState

case class PodPending(pod: Pod) extends ExecutorPodState

sealed trait FinalPodState extends ExecutorPodState

case class PodSucceeded(pod: Pod) extends FinalPodState

case class PodFailed(pod: Pod) extends FinalPodState

case class PodDeleted(pod: Pod) extends FinalPodState

case class PodUnknown(pod: Pod) extends ExecutorPodState
