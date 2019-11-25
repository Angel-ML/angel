package com.tencent.angel.spark.ml.graph.node2vec

import com.tencent.angel.spark.ml.graph.params._
import org.apache.spark.ml.param.Params

trait Node2VecParams extends Params with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasBatchSize with HasWalkLength with HasPValue with HasQValue with HasNeedReplicaEdge
  with HasDegreeBinSize with HasHitRatio with HasPullBatchSize {

  setDefault(srcNodeIdCol, "src")
  setDefault(dstNodeIdCol, "dst")
  setDefault(pValue, 1.0)
  setDefault(qValue, 1.0)
  setDefault(walkLength, 10)
  setDefault(batchSize, 128)
  setDefault(needReplicaEdge, false)
  setDefault(degreeBinSize, 100)
  setDefault(hitRatio, 0.5)
  setDefault(pullBatchSize, 1000)
}
