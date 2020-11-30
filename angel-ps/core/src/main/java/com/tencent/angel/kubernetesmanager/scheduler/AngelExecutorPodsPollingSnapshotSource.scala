package com.tencent.angel.kubernetesmanager.scheduler

import java.util.concurrent.{Future, ScheduledExecutorService, TimeUnit}

import com.tencent.angel.kubernetesmanager.deploy.config.Constants
import com.tencent.angel.kubernetesmanager.deploy.config.Constants.{ANGEL_POD_PS_ROLE, ANGEL_POD_WORKER_ROLE}
import com.tencent.angel.kubernetesmanager.deploy.utils.ThreadUtils
import com.tencent.angel.conf.AngelConf
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._

private[angel] class AngelExecutorPodsPollingSnapshotSource(
                                                             conf: Configuration,
                                                             kubernetesClient: KubernetesClient,
                                                             snapshotsStore: AngelExecutorPodsSnapshotsStore,
                                                             pollingExecutor: ScheduledExecutorService) {

  private final val LOG: Log = LogFactory.getLog(classOf[AngelExecutorPodsPollingSnapshotSource])

  private val executorRole = conf.get(AngelConf.ANGEL_KUBERNETES_EXECUTOR_ROLE,
    AngelConf.DEFAULT_ANGEL_KUBERNETES_EXECUTOR_ROLE)
  private val executorRoleLabel = if (executorRole.equals("ps")) ANGEL_POD_PS_ROLE else ANGEL_POD_WORKER_ROLE

  private val pollingInterval = conf.getInt(AngelConf.ANGEL_KUBERNETES_EXECUTOR_API_POLLING_INTERVAL,
    AngelConf.DEFAULT_ANGEL_KUBERNETES_EXECUTOR_API_POLLING_INTERVAL)

  private var pollingFuture: Future[_] = _

  def start(applicationId: String): Unit = {
    require(pollingFuture == null, "Cannot start polling more than once.")
    LOG.debug(s"Starting to check for executor pod state every $pollingInterval ms.")
    pollingFuture = pollingExecutor.scheduleWithFixedDelay(
      new PollRunnable(applicationId), pollingInterval, pollingInterval, TimeUnit.MILLISECONDS)
  }

  def stop(): Unit = {
    if (pollingFuture != null) {
      pollingFuture.cancel(true)
      pollingFuture = null
    }
    ThreadUtils.shutdown(pollingExecutor)
  }

  private class PollRunnable(applicationId: String) extends Runnable {
    override def run(): Unit = {
      LOG.debug(s"Resynchronizing full executor pod state from Kubernetes.")
      snapshotsStore.replaceSnapshot(kubernetesClient
        .pods()
        .withLabel(Constants.ANGEL_APP_ID_LABEL, applicationId)
        .withLabel(Constants.ANGEL_ROLE_LABEL, executorRoleLabel)
        .list()
        .getItems
        .asScala)
    }
  }

}
