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

import java.util.UUID

import com.tencent.angel.kubernetesmanager.deploy.config.{AngelPod, KubernetesConf, KubernetesRoleSpecificConf}
import io.fabric8.kubernetes.api.model._

private[angel] class LocalDirsFeatureStep(
                                           conf: KubernetesConf[_ <: KubernetesRoleSpecificConf],
                                           defaultLocalDir: String = s"/var/data/angel-${UUID.randomUUID}")
  extends KubernetesFeatureConfigStep {

  private val resolvedLocalDirs = Option(System.getenv("ANGEL_LOCAL_DIRS"))
    .orElse(Option(conf.angelConf.get("angel.local.dir")))
    .getOrElse(defaultLocalDir)
    .split(",")

  override def configurePod(pod: AngelPod): AngelPod = {
    val localDirVolumes = resolvedLocalDirs
      .zipWithIndex
      .map { case (localDir, index) =>
        new VolumeBuilder()
          .withName(s"angel-local-dir-${index + 1}")
          .withNewEmptyDir()
          .endEmptyDir()
          .build()
      }
    val localDirVolumeMounts = localDirVolumes
      .zip(resolvedLocalDirs)
      .map { case (localDirVolume, localDirPath) =>
        new VolumeMountBuilder()
          .withName(localDirVolume.getName)
          .withMountPath(localDirPath)
          .build()
      }
    val podWithLocalDirVolumes = new PodBuilder(pod.pod)
      .editSpec()
      .addToVolumes(localDirVolumes: _*)
      .endSpec()
      .build()
    val containerWithLocalDirVolumeMounts = new ContainerBuilder(pod.container)
      .addNewEnv()
      .withName("ANGEL_LOCAL_DIRS")
      .withValue(resolvedLocalDirs.mkString(","))
      .endEnv()
      .addToVolumeMounts(localDirVolumeMounts: _*)
      .build()
    AngelPod(podWithLocalDirVolumes, containerWithLocalDirVolumeMounts)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
