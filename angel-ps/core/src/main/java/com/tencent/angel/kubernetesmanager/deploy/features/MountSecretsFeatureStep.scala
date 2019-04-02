package com.tencent.angel.kubernetesmanager.deploy.features

import com.tencent.angel.kubernetesmanager.deploy.config.{AngelPod, KubernetesConf, KubernetesRoleSpecificConf}
import io.fabric8.kubernetes.api.model._

private[angel] class MountSecretsFeatureStep(
    kubernetesConf: KubernetesConf[_ <: KubernetesRoleSpecificConf])
  extends KubernetesFeatureConfigStep {
  override def configurePod(pod: AngelPod): AngelPod = {
    val addedVolumes = kubernetesConf
      .roleSecretNamesToMountPaths
      .keys
      .map(secretName =>
        new VolumeBuilder()
          .withName(secretVolumeName(secretName))
          .withNewSecret()
          .withSecretName(secretName)
          .endSecret()
          .build())
    val podWithVolumes = new PodBuilder(pod.pod)
      .editOrNewSpec()
      .addToVolumes(addedVolumes.toSeq: _*)
      .endSpec()
      .build()
    val addedVolumeMounts = kubernetesConf
      .roleSecretNamesToMountPaths
      .map {
        case (secretName, mountPath) =>
          new VolumeMountBuilder()
            .withName(secretVolumeName(secretName))
            .withMountPath(mountPath)
            .build()
      }
    val containerWithMounts = new ContainerBuilder(pod.container)
      .addToVolumeMounts(addedVolumeMounts.toSeq: _*)
      .build()
    AngelPod(podWithVolumes, containerWithMounts)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty

  private def secretVolumeName(secretName: String): String = s"$secretName-volume"
}
