package com.tencent.angel.kubernetesmanager.scheduler

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import io.fabric8.kubernetes.api.model.{ContainerBuilder, PodBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.hadoop.conf.Configuration
import com.tencent.angel.kubernetesmanager.deploy.config.{Constants, KubernetesConf}
import com.tencent.angel.conf.AngelConf
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable
import scala.util.control.{Breaks, NonFatal}

private[angel] class AngelExecutorPodsAllocator(
                                            conf: Configuration,
                                            executorBuilder: KubernetesAngelExecutorBuilder,
                                            kubernetesClient: KubernetesClient,
                                            snapshotsStore: AngelExecutorPodsSnapshotsStore) {

  private final val LOG: Log = LogFactory.getLog(classOf[AngelExecutorPodsAllocator])

  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)

  private val totalExpectedExecutors = new AtomicInteger(0)

  private val podAllocationSize = conf.getInt(AngelConf.ANGEL_KUBERNETES_ALLOCATION_BATCH_SIZE,
    AngelConf.DEFAULT_ANGEL_KUBERNETES_ALLOCATION_BATCH_SIZE)

  private val podAllocationDelay = conf.getInt(AngelConf.ANGEL_KUBERNETES_ALLOCATION_BATCH_DELAY,
    AngelConf.DEFAULT_ANGEL_KUBERNETES_ALLOCATION_BATCH_DELAY)

  private val podCreationTimeout = math.max(podAllocationDelay * 5, 60000)

  private val namespace = conf.get(AngelConf.ANGEL_KUBERNETES_NAMESPACE,
    AngelConf.DEFAULT_ANGEL_KUBERNETES_NAMESPACE)

  private val kubernetesMasterPodName = Option(conf.get(AngelConf.ANGEL_KUBERNETES_MASTER_POD_NAME))

  private val masterPod = kubernetesMasterPodName
    .map(name => Option(kubernetesClient.pods()
      .withName(name)
      .get())
      .getOrElse(throw new Exception(
        s"No pod was found named $kubernetesMasterPodName in the cluster in the " +
          s"namespace $namespace (this was supposed to be the angel master pod.).")))

  // Executor IDs that have been requested from Kubernetes but have not been detected in any
  // snapshot yet. Mapped to the timestamp when they were created.
  private val newlyCreatedExecutors = mutable.Map.empty[Long, Long]

  def start(applicationId: String): Unit = {
    snapshotsStore.addSubscriber(podAllocationDelay) {
      onNewSnapshots(applicationId, _)
    }
  }

  def setTotalExpectedExecutors(total: Int): Unit = totalExpectedExecutors.set(total)

  private def onNewSnapshots(applicationId: String, snapshots: Seq[AngelExecutorPodsSnapshot]): Unit = {
    newlyCreatedExecutors --= snapshots.flatMap(_.executorPods.keys)
    // For all executors we've created against the API but have not seen in a snapshot
    // yet - check the current time. If the current time has exceeded some threshold,
    // assume that the pod was either never created (the API server never properly
    // handled the creation request), or the API server created the pod but we missed
    // both the creation and deletion events. In either case, delete the missing pod
    // if possible, and mark such a pod to be rescheduled below.
    newlyCreatedExecutors.foreach { case (execId, timeCreated) =>
      val currentTime = System.currentTimeMillis()
      if (currentTime - timeCreated > podCreationTimeout) {
        LOG.warn(s"Executor with id $execId was not detected in the Kubernetes" +
          s" cluster after $podCreationTimeout milliseconds despite the fact that a" +
          " previous allocation attempt tried to create it. The executor may have been" +
          " deleted but the application missed the deletion event.")
        try {
          kubernetesClient
            .pods()
            .withLabel(Constants.ANGEL_EXECUTOR_ID_LABEL, execId.toString)
            .delete()
        } catch {
          case NonFatal(t) =>
            println(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        }
        newlyCreatedExecutors -= execId
      } else {
        LOG.debug(s"Executor with id $execId was not found in the Kubernetes cluster since it" +
          s" was created ${currentTime - timeCreated} milliseconds ago.")
      }
    }

    if (snapshots.nonEmpty) {
      // Only need to examine the cluster as of the latest snapshot, the "current" state, to see if
      // we need to allocate more executors or not.
      val latestSnapshot = snapshots.last
      val currentRunningExecutors = latestSnapshot.executorPods.values.count {
        case PodRunning(_) => true
        case _ => false
      }
      val currentPendingExecutors = latestSnapshot.executorPods.values.count {
        case PodPending(_) => true
        case _ => false
      }
      val executorRole = conf.get(AngelConf.ANGEL_KUBERNETES_EXECUTOR_ROLE,
        AngelConf.DEFAULT_ANGEL_KUBERNETES_EXECUTOR_ROLE)
      val currentTotalExpectedExecutors = totalExpectedExecutors.get
      LOG.debug(s"Currently have $currentRunningExecutors running executors and" +
        s" $currentPendingExecutors pending executors. $newlyCreatedExecutors executors" +
        s" have been requested but are pending appearance in the cluster.")
      if (newlyCreatedExecutors.isEmpty
        && currentPendingExecutors == 0
        && currentRunningExecutors < currentTotalExpectedExecutors) {
        val numExecutorsToAllocate = math.min(
          currentTotalExpectedExecutors - currentRunningExecutors, podAllocationSize)
        val loop = new Breaks
        LOG.info(s"Going to request $numExecutorsToAllocate executors from Kubernetes.")
        loop.breakable {
          for ( _ <- 0 until numExecutorsToAllocate) {
            if (executorRole.equals("ps")) {
              val pSAttemptId = KubernetesClusterManager.getContext()
                .getParameterServerManager.getPsAttemptIdBlockingQueue.poll()
              if (pSAttemptId != null) {
                conf.set(Constants.ANGEL_EXECUTOR_ID, pSAttemptId.getPsId.getIndex.toString)
                conf.set(Constants.ANGEL_EXECUTOR_ATTEMPT_ID, pSAttemptId.getIndex.toString)
              } else {
                Thread.sleep(5000)
                loop.break
              }
            } else {
              val workerAttemptId = KubernetesClusterManager.getContext()
                .getWorkerManager.getWorkerAttemptIdBlockingQueue.poll()
              if (workerAttemptId != null) {
                conf.set(Constants.ANGEL_EXECUTOR_ID, workerAttemptId.getWorkerId.getWorkerGroupId.getIndex.toString)
                conf.set(Constants.ANGEL_EXECUTOR_ATTEMPT_ID, workerAttemptId.getIndex.toString)
              } else {
                Thread.sleep(5000)
                loop.break
              }
            }
            val newExecutorId = EXECUTOR_ID_COUNTER.incrementAndGet()
            val executorConf = KubernetesConf.createExecutorConf(
              conf,
              newExecutorId.toString,
              applicationId,
              masterPod)
            val executorPod = executorBuilder.buildFromFeatures(executorConf)
            val resolvedExecutorContainer = new ContainerBuilder(executorPod.container)
              .addNewVolumeMount()
              .withName(Constants.ANGEL_CONF_VOLUME)
              .withMountPath(Constants.ANGEL_CONF_DIR_INTERNAL)
              .endVolumeMount()
              .build()
            val podWithAttachedContainer = new PodBuilder(executorPod.pod)
              .editOrNewSpec()
              .addToContainers(resolvedExecutorContainer)
              .addNewVolume()
              .withName(Constants.ANGEL_CONF_VOLUME)
              .withNewConfigMap()
              .withName(executorConf.appResourceNamePrefix + "-master-conf-map")
              .endConfigMap()
              .endVolume()
              .endSpec()
              .build()
            kubernetesClient.pods().create(podWithAttachedContainer)
            newlyCreatedExecutors(newExecutorId) = System.currentTimeMillis()
            LOG.debug(s"Requested executor with id $newExecutorId from Kubernetes.")
          }
        }
      } else if (currentRunningExecutors >= currentTotalExpectedExecutors) {
        // TODO handle edge cases if we end up with more running executors than expected.
        LOG.debug("Current number of running executors is equal to the number of requested" +
          " executors. Not scaling up further.")
      } else if (newlyCreatedExecutors.nonEmpty || currentPendingExecutors != 0) {
        LOG.debug(s"Still waiting for ${newlyCreatedExecutors.size + currentPendingExecutors}" +
          s" executors to begin running before requesting for more executors. # of executors in" +
          s" pending status in the cluster: $currentPendingExecutors. # of executors that we have" +
          s" created but we have not observed as being present in the cluster yet:" +
          s" ${newlyCreatedExecutors.size}.")
      }
    }
  }
}
