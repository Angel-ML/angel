package com.tencent.angel.spark.ml.graph.algo.embedding.node2vec

import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.ml.param.Params
import org.apache.spark.storage.StorageLevel

trait Node2VecParams extends Params with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasBatchSize with HasWalkLength with HasPValue with HasQValue with HasNeedReplicaEdge with HasPullSizeInMB
  with HasUseBalancePartition with HasIsTrunc with HasUseCache with HasCacheRatio with HasSetCheckPoint with HasLenThreshold
  with HasEpochNum with HasTruncLength with HasStorageLevel with HasWeightCol with HasIsWeighted
  with HasBalancePartitionPercent {

  setDefault(srcNodeIdCol, "src")
  setDefault(dstNodeIdCol, "dst")
  setDefault(weightCol, "weight")
  setDefault(pValue, 1.0)
  setDefault(qValue, 1.0)
  setDefault(walkLength, 10)
  setDefault(batchSize, 128)
  setDefault(needReplicaEdge, false)
  setDefault(isTrunc, false)
  setDefault(useCache, true)
  setDefault(cacheRatio, 0.25)
  setDefault(pullSizeInMB, 10)
  setDefault(setCheckPoint, false)
  setDefault(lenThreshold, 50000.0)
  setDefault(epochNum, 10)
  setDefault(truncLength, 6000)
  setDefault(storageLevel, StorageLevel.MEMORY_ONLY)
  setDefault(isWeighted, false)
  setDefault(useBalancePartition, false)
  setDefault(balancePartitionPercent, 0.7F)
}