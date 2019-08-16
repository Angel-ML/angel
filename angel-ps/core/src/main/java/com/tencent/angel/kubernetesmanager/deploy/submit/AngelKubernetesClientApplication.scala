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

import java.io.{Closeable, StringWriter}
import java.util.{Collections, Properties}

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.kubernetesmanager.deploy.config.{Constants, KubernetesConf, KubernetesMasterSpecificConf}
import com.tencent.angel.kubernetesmanager.deploy.utils.AngelKubernetesClientFactory
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration

import scala.util.control.NonFatal

private[angel] class Client(
                             builder: KubernetesAngelMasterBuilder,
                             kubernetesConf: KubernetesConf[KubernetesMasterSpecificConf],
                             kubernetesClient: KubernetesClient,
                             waitForAppCompletion: Boolean,
                             appName: String,
                             watcher: LoggingPodStatusWatcher,
                             kubernetesResourceNamePrefix: String) {

  private final val LOG: Log = LogFactory.getLog(classOf[KubernetesClientApplication])

  def run(): Unit = {
    val resolvedMasterSpec = builder.buildFromFeatures(kubernetesConf)
    val configMapName = s"$kubernetesResourceNamePrefix-master-conf-map"
    val configMap = buildConfigMap(configMapName, resolvedMasterSpec.systemProperties)
    // The include of the ENV_VAR for "ANGEL_CONF_DIR" is to allow for the
    // Angel command builder to pickup on the Java Options present in the ConfigMap
    val resolvedMasterContainer = new ContainerBuilder(resolvedMasterSpec.pod.container)
      .addNewEnv()
      .withName(Constants.ENV_ANGEL_CONF_DIR)
      .withValue(Constants.ANGEL_CONF_DIR_INTERNAL)
      .endEnv()
      .addNewVolumeMount()
      .withName(Constants.ANGEL_CONF_VOLUME)
      .withMountPath(Constants.ANGEL_CONF_DIR_INTERNAL)
      .endVolumeMount()
      .build()
    val resolvedMasterPod = new PodBuilder(resolvedMasterSpec.pod.pod)
      .editSpec()
      .addToContainers(resolvedMasterContainer)
      .addNewVolume()
      .withName(Constants.ANGEL_CONF_VOLUME)
      .withNewConfigMap()
      .withName(configMapName)
      .endConfigMap()
      .endVolume()
      .endSpec()
      .build()
    tryWithResource(
      kubernetesClient
        .pods()
        .withName(resolvedMasterPod.getMetadata.getName)
        .watch(watcher)) { _ =>
      val createdMasterPod = kubernetesClient.pods().create(resolvedMasterPod)
      try {
        val otherKubernetesResources =
          resolvedMasterSpec.masterKubernetesResources ++ Seq(configMap)
        addAngelMasterOwnerReference(createdMasterPod, otherKubernetesResources)
        kubernetesClient.resourceList(otherKubernetesResources: _*).createOrReplace()
      } catch {
        case NonFatal(e) =>
          kubernetesClient.pods().delete(createdMasterPod)
          throw e
      }

      if (waitForAppCompletion) {
        LOG.info(s"Waiting for application $appName to finish...")
        watcher.awaitCompletion()
        LOG.info(s"Application $appName finished.")
      } else {
        LOG.info(s"Deployed Angel application $appName into Kubernetes.")
      }
    }
  }

  // Add a OwnerReference to the given resources making the angel master pod an owner of them so when
  // the master pod is deleted, the resources are garbage collected.
  private def addAngelMasterOwnerReference(angelMasterPod: Pod, resources: Seq[HasMetadata]): Unit = {
    val masterPodOwnerReference = new OwnerReferenceBuilder()
      .withName(angelMasterPod.getMetadata.getName)
      .withApiVersion(angelMasterPod.getApiVersion)
      .withUid(angelMasterPod.getMetadata.getUid)
      .withKind(angelMasterPod.getKind)
      .withController(true)
      .build()
    resources.foreach { resource =>
      val originalMetadata = resource.getMetadata
      originalMetadata.setOwnerReferences(Collections.singletonList(masterPodOwnerReference))
    }
  }

  private def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }

  // Build a Config Map that will house angel conf properties in a single file.
  private def buildConfigMap(configMapName: String, conf: Map[String, String]): ConfigMap = {
    val properties = new Properties()
    conf.foreach { case (k, v) =>
      properties.setProperty(k, v)
    }
    val propertiesWriter = new StringWriter()
    properties.store(propertiesWriter,
      s"Java properties built from Kubernetes config map with name: $configMapName")
    new ConfigMapBuilder()
      .withNewMetadata()
      .withName(configMapName)
      .endMetadata()
      .addToData(Constants.ANGEL_CONF_FILE_NAME, propertiesWriter.toString)
      .build()
  }


}

/**
  * Main class and entry point of application submission in KUBERNETES mode.
  */
private[angel] class KubernetesClientApplication {

  private final val LOG: Log = LogFactory.getLog(classOf[KubernetesClientApplication])
  private var k8sClient: KubernetesClient = _
  private var angelMasterPodName: String = _


  def run(conf: Configuration): Unit = {
    val kubernetesMaster = conf.get(AngelConf.ANGEL_KUBERNETES_MASTER)
    val appName = conf.get(AngelConf.ANGEL_JOB_NAME, AngelConf.DEFAULT_ANGEL_JOB_NAME)
    val kubernetesAppId = s"angel-${conf.get(AngelConf.ANGEL_KUBERNETES_APP_ID).replaceAll("_", "-")}"
    val waitForAppCompletion = conf.getBoolean(AngelConf.ANGEL_KUBERNETES_WAIT_FOR_APP_COMPLETION,
      AngelConf.DEFAULT_ANGEL_KUBERNETES_WAIT_FOR_APP_COMPLETION)
    val kubernetesResourceNamePrefix = KubernetesClientApplication.getResourceNamePrefix(appName)
    val kubernetesConf = KubernetesConf.createMasterConf(
      conf,
      appName,
      kubernetesResourceNamePrefix,
      kubernetesAppId)
    val builder = new KubernetesAngelMasterBuilder
    val namespace = kubernetesConf.namespace()
    val loggingInterval = if (waitForAppCompletion) Some(conf.getInt(AngelConf.ANGEL_KUBERNETES_REPORT_INTERVAL,
      AngelConf.DEFAULT_ANGEL_KUBERNETES_REPORT_INTERVAL).toLong) else None

    val watcher = new LoggingPodStatusWatcherImpl(kubernetesAppId, loggingInterval)
    tryWithResource(AngelKubernetesClientFactory.createKubernetesClient(
      kubernetesMaster,
      Some(namespace),
      AngelConf.ANGEL_KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX,
      conf,
      None,
      None)) { kubernetesClient =>
      val client = new Client(
        builder,
        kubernetesConf,
        kubernetesClient,
        waitForAppCompletion,
        appName,
        watcher,
        kubernetesResourceNamePrefix)
      k8sClient = kubernetesClient
      angelMasterPodName = kubernetesResourceNamePrefix + "-master"
      client.run()
    }

  }

  def getAngelMasterPodIp: String = {
    var pod: Pod = null
    var podIp: String = null
    LOG.info("waiting for get angel master pod ip, pod name is: " + angelMasterPodName)
    try {
      pod = k8sClient.pods().withName(angelMasterPodName).get()
      podIp = pod.getStatus.getPodIP
    } catch {
      case npe: NullPointerException =>
        Thread.sleep(1000)
      case e: Exception =>
        throw e
    }
    if (podIp != null && !"".equals(podIp) && pod.getStatus.getPhase.equals("Running")) {
      LOG.info("Now angel master pod state phase is Running, return angel master pod ip.")
    }
    podIp
  }

  //todo
  def deleteMasterPod: Unit = {

  }

  private def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }
}

private[angel] object KubernetesClientApplication {

  def getAppName(conf: Configuration): String = conf.get(AngelConf.ANGEL_JOB_NAME, AngelConf.DEFAULT_ANGEL_JOB_NAME)

  def getResourceNamePrefix(appName: String): String = {
    val launchTime = System.currentTimeMillis()
    s"$appName-$launchTime"
      .trim
      .toLowerCase
      .replaceAll("\\s+", "-")
      .replaceAll("\\.", "-")
      .replaceAll("[^a-z0-9\\-]", "")
      .replaceAll("-+", "-")
  }
}
