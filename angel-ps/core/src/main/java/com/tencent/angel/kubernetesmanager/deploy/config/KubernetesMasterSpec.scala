package com.tencent.angel.kubernetesmanager.deploy.config

import io.fabric8.kubernetes.api.model.HasMetadata

private[angel] case class KubernetesMasterSpec(
    pod: AngelPod,
    masterKubernetesResources: Seq[HasMetadata],
    systemProperties: Map[String, String])

private[angel] object KubernetesMasterSpec {
  def initialSpec(initialProps: Map[String, String]): KubernetesMasterSpec = KubernetesMasterSpec(
    AngelPod.initialPod(),
    Seq.empty,
    initialProps)
}
