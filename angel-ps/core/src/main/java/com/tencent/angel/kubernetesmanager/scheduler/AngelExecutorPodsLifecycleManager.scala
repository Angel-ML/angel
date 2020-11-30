package com.tencent.angel.kubernetesmanager.scheduler

import com.tencent.angel.conf.AngelConf
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable

private[angel] class AngelExecutorPodsLifecycleManager(
                                                   conf: Configuration,
                                                   executorBuilder: KubernetesAngelExecutorBuilder,
                                                   kubernetesClient: KubernetesClient,
                                                   snapshotsStore: AngelExecutorPodsSnapshotsStore) {

  private final val LOG: Log = LogFactory.getLog(classOf[AngelExecutorPodsLifecycleManager])

  private val eventProcessingInterval = conf.getInt(AngelConf.ANGEL_KUBERNETES_EXECUTOR_EVENT_PROCESSING_INTERVAL,
    AngelConf.DEFAULT_ANGEL_KUBERNETES_EXECUTOR_EVENT_PROCESSING_INTERVAL).toLong

  def start(): Unit = {
    snapshotsStore.addSubscriber(eventProcessingInterval) {
      onNewSnapshots
    }
  }

  private def onNewSnapshots(snapshots: Seq[AngelExecutorPodsSnapshot]): Unit = {
    val execIdsRemovedInThisRound = mutable.HashSet.empty[Long]
    snapshots.foreach { snapshot =>
      snapshot.executorPods.foreach { case (execId, state) =>
        state match {
          case deleted@PodDeleted(_) =>
            LOG.info(s"Snapshot reported deleted executor with id $execId," +
              s" pod name ${state.pod.getMetadata.getName}")
            execIdsRemovedInThisRound += execId
          case failed@PodFailed(_) =>
            LOG.info(s"Snapshot reported failed executor with id $execId," +
              s" pod name ${state.pod.getMetadata.getName}")
            onFinalNonDeletedState(failed, execId, execIdsRemovedInThisRound)
          case succeeded@PodSucceeded(_) =>
            LOG.info(s"Snapshot reported succeeded executor with id $execId," +
              s" pod name ${state.pod.getMetadata.getName}. Note that succeeded executors are" +
              s" unusual unless Angel specifically informed the executor to exit.")
            onFinalNonDeletedState(succeeded, execId, execIdsRemovedInThisRound)
          case _ =>
        }
      }
    }

    if (execIdsRemovedInThisRound.nonEmpty) {
      LOG.info(s"Removed executors with ids ${execIdsRemovedInThisRound.mkString(",")}" +
        s" from Angel that were either found to be deleted or non-existent in the cluster.")
    }
  }

  private def onFinalNonDeletedState(
      podState: FinalPodState,
      execId: Long,
      execIdsRemovedInRound: mutable.Set[Long]): Unit = {
    removeExecutorFromK8s(podState.pod)
    execIdsRemovedInRound += execId
  }

  private def removeExecutorFromK8s(updatedPod: Pod): Unit = {
    // If deletion failed on a previous try, we can try again if resync informs us the pod
    // is still around.
    // Delete as best attempt - duplicate deletes will throw an exception but the end state
    // of getting rid of the pod is what matters.
      kubernetesClient
        .pods()
        .withName(updatedPod.getMetadata.getName)
        .delete()
  }
}
