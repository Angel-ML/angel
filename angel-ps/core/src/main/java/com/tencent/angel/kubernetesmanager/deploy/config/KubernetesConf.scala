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
package com.tencent.angel.kubernetesmanager.deploy.config

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.kubernetesmanager.deploy.config.Constants._
import com.tencent.angel.kubernetesmanager.deploy.submit.KubernetesClientApplication._
import com.tencent.angel.kubernetesmanager.deploy.utils.KubernetesUtils
import io.fabric8.kubernetes.api.model.Pod
import org.apache.hadoop.conf.Configuration

private[angel] sealed trait KubernetesRoleSpecificConf

private[angel] case class KubernetesMasterSpecificConf(appName: String)
  extends KubernetesRoleSpecificConf

/*
 * Structure containing metadata for Kubernetes logic that builds a angel executor(ps or worker).
 */
private[angel] case class KubernetesExecutorSpecificConf(
                                                          executorId: String,
                                                          masterPod: Option[Pod])
  extends KubernetesRoleSpecificConf

/**
  * Structure containing metadata for Kubernetes logic to build angel pods.
  */
private[angel] case class KubernetesConf[T <: KubernetesRoleSpecificConf](
                                                                           angelConf: Configuration,
                                                                           roleSpecificConf: T,
                                                                           appResourceNamePrefix: String,
                                                                           appId: String,
                                                                           roleLabels: Map[String, String],
                                                                           roleAnnotations: Map[String, String],
                                                                           roleSecretNamesToMountPaths: Map[String, String],
                                                                           roleSecretEnvNamesToKeyRefs: Map[String, String],
                                                                           roleEnvs: Map[String, String],
                                                                           roleVolumes: Iterable[KubernetesVolumeSpec[_ <: KubernetesVolumeSpecificConf]]) {


  def namespace(): String = angelConf.get(AngelConf.ANGEL_KUBERNETES_NAMESPACE,
    AngelConf.DEFAULT_ANGEL_KUBERNETES_NAMESPACE)

  def serviceAccount(): String = angelConf.get(AngelConf.ANGEL_KUBERNETES_SERVICEACCOUNT,
    AngelConf.DEFAULT_ANGEL_KUBERNETES_SERVICEACCOUNT)

  def imagePullPolicy(): String = angelConf.get(AngelConf.ANGEL_KUBERNETES_CONTAINER_IMAGE_PULL_POLICY,
    AngelConf.DEFAULT_ANGEL_KUBERNETES_CONTAINER_IMAGE_PULL_POLICY)
}

private[angel] object KubernetesConf {
  def createMasterConf(
                        angelConf: Configuration,
                        appName: String,
                        appResourceNamePrefix: String,
                        appId: String): KubernetesConf[KubernetesMasterSpecificConf] = {

    val masterCustomLabels = KubernetesUtils.parsePrefixedKeyValuePairs(angelConf,
      AngelConf.ANGEL_KUBERNETES_MASTER_LABEL_PREFIX)
    require(!masterCustomLabels.contains(ANGEL_APP_ID_LABEL), "Label with key " +
      s"$ANGEL_APP_ID_LABEL is not allowed as it is reserved for Angel bookkeeping " +
      "operations.")
    require(!masterCustomLabels.contains(ANGEL_ROLE_LABEL), "Label with key " +
      s"$ANGEL_ROLE_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")
    val masterLabels = masterCustomLabels ++ Map(
      ANGEL_APP_ID_LABEL -> appId,
      ANGEL_ROLE_LABEL -> ANGEL_POD_MASTER_ROLE)
    val masterAnnotations = KubernetesUtils.parsePrefixedKeyValuePairs(angelConf,
      AngelConf.ANGEL_KUBERNETES_MASTER_ANNOTATION_PREFIX)
    val masterSecretNamesToMountPaths = KubernetesUtils.parsePrefixedKeyValuePairs(angelConf,
      AngelConf.ANGEL_KUBERNETES_MASTER_SECRETS_PREFIX)
    val masterSecretEnvNamesToKeyRefs = KubernetesUtils.parsePrefixedKeyValuePairs(angelConf,
      AngelConf.ANGEL_KUBERNETES_MASTER_SECRET_KEY_REF_PREFIX)
    val masterEnvs = KubernetesUtils.parsePrefixedKeyValuePairs(angelConf,
      AngelConf.ANGEL_KUBERNETES_MASTER_ENV_PREFIX)
    val masterVolumes = KubernetesUtils.parseVolumesWithPrefix(angelConf,
      AngelConf.ANGEL_KUBERNETES_MASTER_VOLUMES_PREFIX).map(_.get)
    KubernetesUtils.parseVolumesWithPrefix(angelConf,
      AngelConf.ANGEL_KUBERNETES_EXECUTOR_VOLUMES_PREFIX).map(_.get)

    KubernetesConf(
      angelConf,
      KubernetesMasterSpecificConf(appName),
      appResourceNamePrefix,
      appId,
      masterLabels,
      masterAnnotations,
      masterSecretNamesToMountPaths,
      masterSecretEnvNamesToKeyRefs,
      masterEnvs,
      masterVolumes)
  }

  def createExecutorConf(
                          angelConf: Configuration,
                          executorId: String,
                          appId: String,
                          masterPod: Option[Pod]): KubernetesConf[KubernetesExecutorSpecificConf] = {
    val executorCustomLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      angelConf, AngelConf.ANGEL_KUBERNETES_EXECUTOR_LABEL_PREFIX)
    require(
      !executorCustomLabels.contains(ANGEL_APP_ID_LABEL),
      s"Custom executor labels cannot contain $ANGEL_APP_ID_LABEL as it is reserved for Angel.")
    require(
      !executorCustomLabels.contains(ANGEL_ROLE_LABEL),
      s"Custom executor labels cannot contain $ANGEL_ROLE_LABEL as it is reserved for Angel.")
    require(
      !executorCustomLabels.contains(ANGEL_EXECUTOR_ID_LABEL),
      s"Custom executor labels cannot contain $ANGEL_EXECUTOR_ID_LABEL as it is reserved for" +
        " Angel.")
    val role = angelConf.get(AngelConf.ANGEL_KUBERNETES_EXECUTOR_ROLE,
      AngelConf.DEFAULT_ANGEL_KUBERNETES_EXECUTOR_ROLE)
    var executorLabels = Map.empty[String, String]
    role match {
      case "ps" =>
        executorLabels = Map(
          ANGEL_EXECUTOR_ID_LABEL -> executorId,
          ANGEL_APP_ID_LABEL -> appId,
          ANGEL_ROLE_LABEL -> ANGEL_POD_PS_ROLE) ++
          executorCustomLabels
      case "worker" =>
        executorLabels = Map(
          ANGEL_EXECUTOR_ID_LABEL -> executorId,
          ANGEL_APP_ID_LABEL -> appId,
          ANGEL_ROLE_LABEL -> ANGEL_POD_WORKER_ROLE) ++
          executorCustomLabels
    }
    val executorAnnotations = KubernetesUtils.parsePrefixedKeyValuePairs(
      angelConf, AngelConf.ANGEL_KUBERNETES_EXECUTOR_ANNOTATION_PREFIX)
    val executorMountSecrets = KubernetesUtils.parsePrefixedKeyValuePairs(
      angelConf, AngelConf.ANGEL_KUBERNETES_EXECUTOR_SECRETS_PREFIX)
    val executorEnvSecrets = KubernetesUtils.parsePrefixedKeyValuePairs(
      angelConf, AngelConf.ANGEL_KUBERNETES_EXECUTOR_SECRET_KEY_REF_PREFIX)
    val executorEnv = KubernetesUtils.parsePrefixedKeyValuePairs(angelConf,
      AngelConf.ANGEL_KUBERNETES_EXECUTOR_ENV_PREFIX)
    val executorVolumes = KubernetesUtils.parseVolumesWithPrefix(
      angelConf, AngelConf.ANGEL_KUBERNETES_EXECUTOR_VOLUMES_PREFIX).map(_.get)
    val appResourceNamePrefix = angelConf.get(AngelConf.ANGEL_KUBERNETES_EXECUTOR_POD_NAME_PREFIX,
      getResourceNamePrefix(getAppName(angelConf)))
    KubernetesConf(
      angelConf,
      KubernetesExecutorSpecificConf(executorId, masterPod),
      appResourceNamePrefix,
      appId,
      executorLabels,
      executorAnnotations,
      executorMountSecrets,
      executorEnvSecrets,
      executorEnv,
      executorVolumes)
  }

}
