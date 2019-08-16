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
package com.tencent.angel.kubernetesmanager.scheduler

import com.tencent.angel.kubernetesmanager.deploy.config._
import com.tencent.angel.kubernetesmanager.deploy.features._

private[angel] class KubernetesAngelExecutorBuilder(
                                                     provideBasicStep: (KubernetesConf[KubernetesExecutorSpecificConf])
                                                       => BasicAngelExecutorFeatureStep =
                                                     new BasicAngelExecutorFeatureStep(_),
                                                     provideSecretsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf])
                                                       => MountSecretsFeatureStep =
                                                     new MountSecretsFeatureStep(_),
                                                     provideEnvSecretsStep:
                                                     (KubernetesConf[_ <: KubernetesRoleSpecificConf] => EnvSecretsFeatureStep) =
                                                     new EnvSecretsFeatureStep(_),
                                                     provideLocalDirsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf])
                                                       => LocalDirsFeatureStep =
                                                     new LocalDirsFeatureStep(_),
                                                     provideVolumesStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf]
                                                       => MountVolumesFeatureStep) =
                                                     new MountVolumesFeatureStep(_)) {

  def buildFromFeatures(
                         kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf]): AngelPod = {

    val baseFeatures = Seq(provideBasicStep(kubernetesConf), provideLocalDirsStep(kubernetesConf))
    val secretFeature = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
      Seq(provideSecretsStep(kubernetesConf))
    } else Nil
    val secretEnvFeature = if (kubernetesConf.roleSecretEnvNamesToKeyRefs.nonEmpty) {
      Seq(provideEnvSecretsStep(kubernetesConf))
    } else Nil
    val volumesFeature = if (kubernetesConf.roleVolumes.nonEmpty) {
      Seq(provideVolumesStep(kubernetesConf))
    } else Nil

    val allFeatures = baseFeatures ++ secretFeature ++ secretEnvFeature ++ volumesFeature

    var executorPod = AngelPod.initialPod()
    for (feature <- allFeatures) {
      executorPod = feature.configurePod(executorPod)
    }
    executorPod
  }
}
