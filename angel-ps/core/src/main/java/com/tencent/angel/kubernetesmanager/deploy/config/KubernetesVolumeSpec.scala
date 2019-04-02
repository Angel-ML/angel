package com.tencent.angel.kubernetesmanager.deploy.config

private[angel] sealed trait KubernetesVolumeSpecificConf

private[angel] case class KubernetesHostPathVolumeConf(
    hostPath: String)
  extends KubernetesVolumeSpecificConf

private[angel] case class KubernetesPVCVolumeConf(
    claimName: String)
  extends KubernetesVolumeSpecificConf

private[angel] case class KubernetesEmptyDirVolumeConf(
    medium: Option[String],
    sizeLimit: Option[String])
  extends KubernetesVolumeSpecificConf

private[angel] case class KubernetesVolumeSpec[T <: KubernetesVolumeSpecificConf](
    volumeName: String,
    mountPath: String,
    mountReadOnly: Boolean,
    volumeConf: T)
