package com.tencent.angel.kubernetesmanager.deploy.features

import com.tencent.angel.kubernetesmanager.deploy.config.{AngelPod, KubernetesConf, KubernetesExecutorSpecificConf}

import scala.collection.JavaConverters._
import io.fabric8.kubernetes.api.model._
import com.tencent.angel.kubernetesmanager.deploy.config.Constants
import com.tencent.angel.conf.AngelConf


private[angel] class BasicAngelExecutorFeatureStep(
    kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf])
  extends KubernetesFeatureConfigStep {

  private val executorRole = kubernetesConf.angelConf.get(AngelConf.ANGEL_KUBERNETES_EXECUTOR_ROLE,
    AngelConf.DEFAULT_ANGEL_KUBERNETES_EXECUTOR_ROLE)

  private val executorExtraClasspath = Option(kubernetesConf.angelConf.get(AngelConf.ANGEL_KUBERNETES_EXECUTOR_EXTRA_CALSSPATH))

  private val executorContainerImage = Option(kubernetesConf.angelConf.get(AngelConf.ANGEL_KUBERNETES_CONTAINER_IMAGE))
    .getOrElse("Must specify the executor container image")

  private val executorPodNamePrefix = kubernetesConf.appResourceNamePrefix

  private val executorMemoryMiB = if (executorRole.equals("ps")) kubernetesConf.angelConf.getInt(AngelConf.ANGEL_PS_MEMORY_GB,
    AngelConf.DEFAULT_ANGEL_PS_MEMORY_GB) * 1024 else kubernetesConf.angelConf.getInt(AngelConf.ANGEL_WORKER_MEMORY_GB,
    AngelConf.DEFAULT_ANGEL_WORKER_MEMORY_GB) * 1024

  private val executorCores = if (executorRole.equals("ps")) kubernetesConf.angelConf.getInt(AngelConf.ANGEL_PS_CPU_VCORES,
    AngelConf.DEFAULT_ANGEL_PS_CPU_VCORES) else kubernetesConf.angelConf.getInt(AngelConf.ANGEL_WORKER_CPU_VCORES,
    AngelConf.DEFAULT_ANGEL_WORKER_CPU_VCORES)

  private val executorLimitCores = if (executorRole.equals("ps")) Option(kubernetesConf.angelConf
    .get(AngelConf.ANGEL_KUBERNETES_PS_LIMIT_CORES)) else Option(kubernetesConf.angelConf
    .get(AngelConf.ANGEL_KUBERNETES_WORKER_LIMIT_CORES))

  override def configurePod(pod: AngelPod): AngelPod = {
    //val name = s"$executorPodNamePrefix-$executorRole-${kubernetesConf.roleSpecificConf.executorId}"
    val executorId = kubernetesConf.angelConf.get(Constants.ANGEL_EXECUTOR_ID)
    val executorAttemptId = kubernetesConf.angelConf.get(Constants.ANGEL_EXECUTOR_ATTEMPT_ID)
    val name = s"$executorPodNamePrefix-$executorRole-attempt-$executorId-$executorAttemptId"

    // hostname must be no longer than 63 characters, so take the last 63 characters of the pod
    // name as the hostname.  This preserves uniqueness since the end of name contains
    // executorId
    val hostname = name.substring(Math.max(0, name.length - 63))
    val executorMemoryQuantity = new QuantityBuilder(false)
      .withAmount(s"${executorMemoryMiB}Mi")
      .build()
    val executorCpuQuantity = new QuantityBuilder(false)
      .withAmount(executorCores.toString)
      .build()
    val executorExtraClasspathEnv = executorExtraClasspath.map { cp =>
      new EnvVarBuilder()
        .withName(Constants.ENV_CLASSPATH)
        .withValue(cp)
        .build()
    }
    val executorEnv = (Seq(
      (Constants.ENV_MASTER_BIND_ADDRESS, kubernetesConf.angelConf.get(AngelConf.ANGEL_KUBERNETES_MASTER_POD_IP)),
      (Constants.ENV_MASTER_BIND_PORT, kubernetesConf.angelConf.getInt(AngelConf.ANGEL_KUBERNETES_MASTER_PORT,
        AngelConf.DEFAULT_ANGEL_KUBERNETES_MASTER_PORT).toString),
      (Constants.ENV_ANGEL_USER_TASK, kubernetesConf.angelConf.get(AngelConf.ANGEL_TASK_USER_TASKCLASS,
        AngelConf.DEFAULT_ANGEL_TASK_USER_TASKCLASS)),
      (Constants.ENV_ANGEL_WORKERGROUP_NUMBER, kubernetesConf.angelConf.get(AngelConf.ANGEL_WORKERGROUP_ACTUAL_NUM)),
      (Constants.ENV_ANGEL_TASK_NUMBER, kubernetesConf.angelConf.get(AngelConf.ANGEL_TASK_ACTUAL_NUM)),
      (Constants.ENV_EXECUTOR_CORES, executorCores.toString),
      (Constants.ENV_EXECUTOR_MEMORY, s"${executorMemoryMiB}M"),
      (Constants.ENV_APPLICATION_ID, kubernetesConf.appId),
      (Constants.ENV_ANGEL_CONF_DIR, Constants.ANGEL_CONF_DIR_INTERNAL),
      (Constants.ENV_EXECUTOR_ID, executorId),
      (Constants.ENV_EXECUTOR_ATTEMPT_ID, executorAttemptId)) ++
      kubernetesConf.roleEnvs)
      .map(env => new EnvVarBuilder()
        .withName(env._1)
        .withValue(env._2)
        .build()
      ) ++ Seq(
      new EnvVarBuilder()
        .withName(Constants.ENV_EXECUTOR_POD_IP)
        .withValueFrom(new EnvVarSourceBuilder()
          .withNewFieldRef("v1", "status.podIP")
          .build())
        .build()
    ) ++ executorExtraClasspathEnv.toSeq

    val executorContainer = new ContainerBuilder(pod.container)
      .withName(executorRole)
      .withImage(executorContainerImage)
      .withImagePullPolicy(kubernetesConf.imagePullPolicy())
        .withNewResources()
        .addToRequests("memory", executorMemoryQuantity)
        .addToLimits("memory", executorMemoryQuantity)
        .addToRequests("cpu", executorCpuQuantity)
        .endResources()
      .addAllToEnv(executorEnv.asJava)
      .addToArgs(executorRole)
      .build()
    val containerWithLimitCores = executorLimitCores.map { limitCores =>
      val executorCpuLimitQuantity = new QuantityBuilder(false)
        .withAmount(limitCores)
        .build()
      new ContainerBuilder(executorContainer)
        .editResources()
          .addToLimits("cpu", executorCpuLimitQuantity)
          .endResources()
        .build()
    }.getOrElse(executorContainer)
    val masterPod = kubernetesConf.roleSpecificConf.masterPod
    val ownerReference = masterPod.map(pod =>
      new OwnerReferenceBuilder()
        .withController(true)
        .withApiVersion(pod.getApiVersion)
        .withKind(pod.getKind)
        .withName(pod.getMetadata.getName)
        .withUid(pod.getMetadata.getUid)
        .build())
    val executorPod = new PodBuilder(pod.pod)
      .editOrNewMetadata()
        .withName(name)
        .withLabels(kubernetesConf.roleLabels.asJava)
        .withAnnotations(kubernetesConf.roleAnnotations.asJava)
        .addToOwnerReferences(ownerReference.toSeq: _*)
        .endMetadata()
      .editOrNewSpec()
        .withHostname(hostname)
        .withRestartPolicy("Never")
        .endSpec()
      .editOrNewSpec()
        .withServiceAccount(kubernetesConf.serviceAccount())
        .withServiceAccountName(kubernetesConf.serviceAccount())
      .endSpec()
      .build()

    AngelPod(executorPod, containerWithLimitCores)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
