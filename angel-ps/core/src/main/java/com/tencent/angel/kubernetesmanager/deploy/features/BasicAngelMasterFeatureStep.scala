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

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.kubernetesmanager.deploy.config.{AngelPod, Constants, KubernetesConf, KubernetesMasterSpecificConf}
import io.fabric8.kubernetes.api.model._

import scala.collection.JavaConverters._
import scala.collection.mutable

private[angel] class BasicAngelMasterFeatureStep(
                                                  conf: KubernetesConf[KubernetesMasterSpecificConf])
  extends KubernetesFeatureConfigStep {

  private val masterPodName = s"${conf.appResourceNamePrefix}-master"

  private val masterExtraClasspath = Option(conf.angelConf.get(AngelConf.ANGEL_KUBERNETES_MASTER_EXTRA_CALSSPATH))

  private val masterContainerImage = Option(conf.angelConf.get(AngelConf.ANGEL_KUBERNETES_CONTAINER_IMAGE))
    .getOrElse(throw new Exception("Must specify the container image"))

  // CPU settings
  private val masterCpuCores = conf.angelConf
    .getInt(AngelConf.ANGEL_AM_CPU_VCORES, AngelConf.DEFAULT_ANGEL_AM_CPU_VCORES)
  private val masterLimitCores = Option(conf.angelConf.get(AngelConf.ANGEL_KUBERNETES_MASTER_LIMIT_CORES))

  // Memory settings
  private val masterMemoryMiB = conf.angelConf
    .getInt(AngelConf.ANGEL_AM_MEMORY_GB, AngelConf.DEFAULT_ANGEL_AM_MEMORY_GB) * 1024

  override def configurePod(pod: AngelPod): AngelPod = {
    val masterCustomEnvs = conf.roleEnvs
      .toSeq
      .map { env =>
        new EnvVarBuilder()
          .withName(env._1)
          .withValue(env._2)
          .build()
      }

    val masterExtraClasspathEnv = masterExtraClasspath.map { cp =>
      new EnvVarBuilder()
        .withName(Constants.ENV_CLASSPATH)
        .withValue(cp)
        .build()
    }

    val masterCpuQuantity = new QuantityBuilder(false)
      .withAmount(masterCpuCores.toString)
      .build()
    val masterMemoryQuantity = new QuantityBuilder(false)
      .withAmount(s"${masterMemoryMiB}Mi")
      .build()
    val maybeCpuLimitQuantity = masterLimitCores.map { limitCores =>
      ("cpu", new QuantityBuilder(false).withAmount(limitCores).build())
    }

    val masterPort = conf.angelConf.getInt(AngelConf.ANGEL_KUBERNETES_MASTER_PORT, AngelConf.DEFAULT_ANGEL_KUBERNETES_MASTER_PORT)
    val masterContainer = new ContainerBuilder(pod.container)
      .withName(Constants.MASTER_CONTAINER_NAME)
      .withImage(masterContainerImage)
      .withImagePullPolicy(conf.imagePullPolicy())
      .addNewPort()
      .withName(Constants.MASTER_PORT_NAME)
      .withContainerPort(masterPort)
      .withProtocol("TCP")
      .endPort()
      .addAllToEnv((masterCustomEnvs ++ masterExtraClasspathEnv.toSeq).asJava)
      .addNewEnv()
      .withName(Constants.ENV_MASTER_BIND_ADDRESS)
      .withValueFrom(new EnvVarSourceBuilder()
        .withNewFieldRef("v1", "status.podIP")
        .build())
      .endEnv()
      .addNewEnv()
      .withName(Constants.ENV_MASTER_MEMORY)
      .withValue(s"${masterMemoryMiB}M")
      .endEnv()
      .addNewEnv()
      .withName(Constants.ENV_MASTER_POD_NAME)
      .withValue(masterPodName)
      .endEnv()
      .addNewEnv()
      .withName(Constants.ENV_APPLICATION_ID)
      .withValue(conf.appId)
      .endEnv()
      .addNewEnv()
      .withName(Constants.ENV_EXECUTOR_POD_NAME_PREFIX)
      .withValue(conf.appResourceNamePrefix)
      .endEnv()
      .withNewResources()
      .addToRequests("cpu", masterCpuQuantity)
      .addToLimits(maybeCpuLimitQuantity.toMap.asJava)
      .addToRequests("memory", masterMemoryQuantity)
      .addToLimits("memory", masterMemoryQuantity)
      .endResources()
      .build()

    val masterPod = new PodBuilder(pod.pod)
      .editOrNewMetadata()
      .withName(masterPodName)
      .addToLabels(conf.roleLabels.asJava)
      .addToAnnotations(conf.roleAnnotations.asJava)
      .endMetadata()
      .withNewSpec()
      .withRestartPolicy("Never")
      .endSpec()
      .editOrNewSpec()
      .withServiceAccount(conf.serviceAccount())
      .withServiceAccountName(conf.serviceAccount())
      .endSpec()
      .build()

    AngelPod(masterPod, masterContainer)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    val additionalProps = mutable.Map(
      "angel.kubernetes.master.pod.name" -> masterPodName,
      "angel.app.id" -> conf.appId,
      AngelConf.ANGEL_KUBERNETES_EXECUTOR_POD_NAME_PREFIX -> conf.appResourceNamePrefix)
    additionalProps.toMap
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
