package com.tencent.angel.kubernetesmanager.deploy.features

import com.tencent.angel.kubernetesmanager.deploy.config.{AngelPod, KubernetesConf, KubernetesMasterSpecificConf}
import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata}

private[angel] class JavaMasterFeatureStep(
                                            kubernetesConf: KubernetesConf[KubernetesMasterSpecificConf])
  extends KubernetesFeatureConfigStep {
  override def configurePod(pod: AngelPod): AngelPod = {
    val withMasterArgs = new ContainerBuilder(pod.container)
      .addToArgs("master")
      .build()
    AngelPod(pod.pod, withMasterArgs)
  }
  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
