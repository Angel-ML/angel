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

import com.tencent.angel.kubernetesmanager.deploy.config._
import io.fabric8.kubernetes.api.model._

private[angel] class MountVolumesFeatureStep(
                                              kubernetesConf: KubernetesConf[_ <: KubernetesRoleSpecificConf])
  extends KubernetesFeatureConfigStep {

  override def configurePod(pod: AngelPod): AngelPod = {
    val (volumeMounts, volumes) = constructVolumes(kubernetesConf.roleVolumes).unzip

    val podWithVolumes = new PodBuilder(pod.pod)
      .editSpec()
      .addToVolumes(volumes.toSeq: _*)
      .endSpec()
      .build()

    val containerWithVolumeMounts = new ContainerBuilder(pod.container)
      .addToVolumeMounts(volumeMounts.toSeq: _*)
      .build()

    AngelPod(podWithVolumes, containerWithVolumeMounts)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty

  private def constructVolumes(
                                volumeSpecs: Iterable[KubernetesVolumeSpec[_ <: KubernetesVolumeSpecificConf]]
                              ): Iterable[(VolumeMount, Volume)] = {
    volumeSpecs.map { spec =>
      val volumeMount = new VolumeMountBuilder()
        .withMountPath(spec.mountPath)
        .withReadOnly(spec.mountReadOnly)
        .withName(spec.volumeName)
        .build()

      val volumeBuilder = spec.volumeConf match {
        case KubernetesHostPathVolumeConf(hostPath) =>
          new VolumeBuilder()
            .withHostPath(new HostPathVolumeSource(hostPath))

        case KubernetesPVCVolumeConf(claimName) =>
          new VolumeBuilder()
            .withPersistentVolumeClaim(
              new PersistentVolumeClaimVolumeSource(claimName, spec.mountReadOnly))

        case KubernetesEmptyDirVolumeConf(medium, sizeLimit) =>
          new VolumeBuilder()
            .withEmptyDir(
              new EmptyDirVolumeSource(medium.getOrElse(""),
                new Quantity(sizeLimit.orNull)))
      }

      val volume = volumeBuilder.withName(spec.volumeName).build()

      (volumeMount, volume)
    }
  }
}
