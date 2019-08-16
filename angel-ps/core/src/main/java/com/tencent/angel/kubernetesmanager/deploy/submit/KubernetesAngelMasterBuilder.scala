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
package com.tencent.angel.kubernetesmanager.deploy.submit

import com.tencent.angel.kubernetesmanager.deploy.config.{KubernetesConf, KubernetesMasterSpec, KubernetesMasterSpecificConf, KubernetesRoleSpecificConf}
import com.tencent.angel.kubernetesmanager.deploy.features._

private[angel] class KubernetesAngelMasterBuilder(
                                                   provideBasicStep: (KubernetesConf[KubernetesMasterSpecificConf]) => BasicAngelMasterFeatureStep =
                                                   new BasicAngelMasterFeatureStep(_),
                                                   provideSecretsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf]
                                                     => MountSecretsFeatureStep) =
                                                   new MountSecretsFeatureStep(_),
                                                   provideEnvSecretsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf]
                                                     => EnvSecretsFeatureStep) =
                                                   new EnvSecretsFeatureStep(_),
                                                   provideLocalDirsStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf])
                                                     => LocalDirsFeatureStep =
                                                   new LocalDirsFeatureStep(_),
                                                   provideVolumesStep: (KubernetesConf[_ <: KubernetesRoleSpecificConf]
                                                     => MountVolumesFeatureStep) =
                                                   new MountVolumesFeatureStep(_),
                                                   provideJavaStep: (KubernetesConf[KubernetesMasterSpecificConf]
                                                     => JavaMasterFeatureStep) =
                                                   new JavaMasterFeatureStep(_)) {

  def buildFromFeatures(
                         kubernetesConf: KubernetesConf[KubernetesMasterSpecificConf]): KubernetesMasterSpec = {
    val baseFeatures = Seq(
      provideBasicStep(kubernetesConf),
      provideLocalDirsStep(kubernetesConf))

    val secretFeature = if (kubernetesConf.roleSecretNamesToMountPaths.nonEmpty) {
      Seq(provideSecretsStep(kubernetesConf))
    } else Nil
    val envSecretFeature = if (kubernetesConf.roleSecretEnvNamesToKeyRefs.nonEmpty) {
      Seq(provideEnvSecretsStep(kubernetesConf))
    } else Nil
    val volumesFeature = if (kubernetesConf.roleVolumes.nonEmpty) {
      Seq(provideVolumesStep(kubernetesConf))
    } else Nil

    val bindingsStep = provideJavaStep(kubernetesConf)

    val allFeatures = (baseFeatures :+ bindingsStep) ++
      secretFeature ++ envSecretFeature ++ volumesFeature
    import scala.collection.JavaConverters._
    var spec = KubernetesMasterSpec.initialSpec(kubernetesConf.angelConf.iterator()
      .asScala.map(x => (x.getKey, x.getValue)).toArray.toMap)
    for (feature <- allFeatures) {
      val configuredPod = feature.configurePod(spec.pod)
      val addedSystemProperties = feature.getAdditionalPodSystemProperties()
      val addedResources = feature.getAdditionalKubernetesResources()
      spec = KubernetesMasterSpec(
        configuredPod,
        spec.masterKubernetesResources ++ addedResources,
        spec.systemProperties ++ addedSystemProperties)
    }
    spec
  }
}
