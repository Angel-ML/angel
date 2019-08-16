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
