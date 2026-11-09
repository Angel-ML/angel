package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasClusterIdx extends Params {
  /**
    * Param for cluster index in the input table.
    *
    * @group param
    */
  final val clusterIdx = new IntParam(this, "clusterIdx", "param for cluster index in the input table")
  
  /** @group getParam */
  final def getClusterIdx: Int = $(clusterIdx)
  
  setDefault(clusterIdx, 0)
  
  /** @group setParam */
  final def setClusterIdx(m: Int): this.type = set(clusterIdx, m)
}