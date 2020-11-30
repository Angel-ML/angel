package com.tencent.angel.kubernetesmanager.scheduler

import com.tencent.angel.kubernetesmanager.deploy.config._
import com.tencent.angel.kubernetesmanager.deploy.features._

private[angel] class KubernetesAngelExecutorBuilder(
                                                     provideBasicStep: (KubernetesConf [KubernetesExecutorSpecificConf])
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
