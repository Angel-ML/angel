package com.tencent.angel.spark.ml.tree.kdtree.trainer

import com.tencent.angel.spark.ml.tree.util.RangeBitSet

import scala.collection.mutable
import FPKDTTrainerWrapper._
import com.tencent.angel.spark.ml.tree.kdtree.psmodel.KDTreePSModel
import com.tencent.angel.spark.ml.tree.kdtree.tree.KDTSplit

class FPKDTTrainerWrapper private(private[trainer] val workerId: Int) {


  private[trainer] def createNewTree() = trainers(workerId).createNewTree()

  private[trainer] def findSplits() = trainers(workerId).findSplits()

  private[trainer] def getSplitResults(splits: Seq[(Int, Int, Int, KDTSplit)]) =
    trainers(workerId).getSplitResults(splits)

  private[trainer] def splitNodes(splits: Seq[(Int, Int, Int, KDTSplit)], splitResults: Seq[(Int, RangeBitSet)]) =
    trainers(workerId).splitNodes(splits, splitResults)

  private[trainer] def setAsLeaf(nid: Int) = trainers(workerId).setAsLeaf(nid)

  private[trainer] def finishTree() = trainers(workerId).finishTree()

  private[trainer] def getTree()= trainers(workerId).getTree()

  private[trainer] def getInstanceInfo()= trainers(workerId).getInstanceInfo()

  private[trainer] def getTreeData(model: KDTreePSModel, bcGroupIdToFid: Array[Int],
                                   featureNum: Int, batchSize: Int = 1000)=
    trainers(workerId).getTreeData(model, bcGroupIdToFid, featureNum, batchSize)

}

object FPKDTTrainerWrapper {
  @transient private val trainers = mutable.Map[Int, FPKDTTrainer]()

  private[trainer] def apply(workerId: Int, trainer: FPKDTTrainer): FPKDTTrainerWrapper = {
    trainer.synchronized {
      require(!trainers.contains(workerId), s"Id $workerId already exists")
      trainers += workerId -> trainer
      new FPKDTTrainerWrapper(workerId)
    }
  }
}
