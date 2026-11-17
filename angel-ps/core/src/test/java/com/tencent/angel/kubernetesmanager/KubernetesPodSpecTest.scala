package com.tencent.angel.kubernetesmanager

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.kubernetesmanager.deploy.config.{AngelPod, Constants, KubernetesConf}
import com.tencent.angel.kubernetesmanager.deploy.features.{BasicAngelExecutorFeatureStep, BasicAngelMasterFeatureStep}
import io.fabric8.kubernetes.api.model.{Container, Pod, PodBuilder}
import org.apache.hadoop.conf.Configuration
import org.junit.Assert._
import org.junit.Test
import scala.collection.JavaConverters._

class KubernetesPodSpecTest {
  private def configuration(): Configuration = {
    val conf = new Configuration(false)
    conf.set(AngelConf.ANGEL_KUBERNETES_CONTAINER_IMAGE, "example/angel:3.4.0")
    conf.set(AngelConf.ANGEL_KUBERNETES_NAMESPACE, "angel-test")
    conf.set(AngelConf.ANGEL_KUBERNETES_MASTER_POD_NAME, "test-master")
    conf.set(AngelConf.ANGEL_KUBERNETES_MASTER_POD_IP, "10.1.0.1")
    conf.setInt(AngelConf.ANGEL_KUBERNETES_MASTER_PORT, 19078)
    conf.set(AngelConf.ANGEL_KUBERNETES_EXECUTOR_POD_NAME_PREFIX, "test")
    conf.set(Constants.ANGEL_EXECUTOR_ID, "2")
    conf.set(Constants.ANGEL_EXECUTOR_ATTEMPT_ID, "1")
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_ACTUAL_NUM, 3)
    conf.setInt(AngelConf.ANGEL_TASK_ACTUAL_NUM, 6)
    conf
  }

  @Test
  def apiUrlHasAnExplicitPortForLegacyWebsocketOrigin(): Unit = {
    val factory = com.tencent.angel.kubernetesmanager.deploy.utils.AngelKubernetesClientFactory
    assertEquals("https://kubernetes.default.svc:443", factory.normalizeMasterUrl("https://kubernetes.default.svc"))
    assertEquals("http://localhost:80/", factory.normalizeMasterUrl("http://localhost/"))
    assertEquals("https://localhost:6443", factory.normalizeMasterUrl("https://localhost:6443"))
    assertEquals("https://[::1]:443", factory.normalizeMasterUrl("https://[::1]"))
  }

  @Test
  def masterUsesConfiguredImagePortAndPodIdentity(): Unit = {
    val conf = configuration()
    val spec = KubernetesConf.createMasterConf(conf, "test", "test", "angel-test-app")
    val result = new BasicAngelMasterFeatureStep(spec).configurePod(AngelPod(new Pod(), new Container()))
    assertEquals("test-master", result.pod.getMetadata.getName)
    assertEquals("example/angel:3.4.0", result.container.getImage)
    assertEquals(19078, result.container.getPorts.get(0).getContainerPort.intValue())
    val environment = result.container.getEnv.asScala.map(e => e.getName -> e).toMap
    assertEquals("test-master", environment(Constants.ENV_MASTER_POD_NAME).getValue)
    assertEquals("status.podIP", environment(Constants.ENV_MASTER_BIND_ADDRESS).getValueFrom.getFieldRef.getFieldPath)
    assertEquals("master", result.pod.getMetadata.getLabels.get(Constants.ANGEL_ROLE_LABEL))
  }

  @Test
  def executorRolesCarryBusinessAttemptsAndMasterOwner(): Unit = {
    val master = new PodBuilder().withApiVersion("v1").withKind("Pod")
      .withNewMetadata().withName("test-master").withUid("test-uid").endMetadata().build()
    for (role <- Seq("ps", "worker")) {
      val conf = configuration()
      conf.set(AngelConf.ANGEL_KUBERNETES_EXECUTOR_ROLE, role)
      val spec = KubernetesConf.createExecutorConf(conf, "99", "angel-test-app", Some(master))
      val result = new BasicAngelExecutorFeatureStep(spec).configurePod(AngelPod(new Pod(), new Container()))
      assertEquals(s"test-$role-attempt-2-1", result.pod.getMetadata.getName)
      assertEquals(role, result.container.getArgs.get(0))
      assertEquals(role, result.pod.getMetadata.getLabels.get(Constants.ANGEL_ROLE_LABEL))
      assertEquals("angel-test-app", result.pod.getMetadata.getLabels.get(Constants.ANGEL_APP_ID_LABEL))
      assertEquals("test-uid", result.pod.getMetadata.getOwnerReferences.get(0).getUid)
      val environment = result.container.getEnv.asScala.map(e => e.getName -> e.getValue).toMap
      assertEquals("2", environment(Constants.ENV_EXECUTOR_ID))
      assertEquals("1", environment(Constants.ENV_EXECUTOR_ATTEMPT_ID))
      assertEquals("10.1.0.1", environment(Constants.ENV_MASTER_BIND_ADDRESS))
      assertEquals("19078", environment(Constants.ENV_MASTER_BIND_PORT))
      assertEquals("3", environment(Constants.ENV_ANGEL_WORKERGROUP_NUMBER))
      assertEquals("6", environment(Constants.ENV_ANGEL_TASK_NUMBER))
    }
  }
}
