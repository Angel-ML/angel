package com.tencent.angel.kubernetesmanager.deploy.config

import io.fabric8.kubernetes.api.model.{Container, ContainerBuilder, Pod, PodBuilder}

private[angel] case class AngelPod(pod: Pod, container: Container)

private[angel] object AngelPod {
  def initialPod(): AngelPod = {
    AngelPod(
      new PodBuilder()
        .withNewMetadata()
        .endMetadata()
        .withNewSpec()
        .endSpec()
        .build(),
      new ContainerBuilder().build())
  }
}
