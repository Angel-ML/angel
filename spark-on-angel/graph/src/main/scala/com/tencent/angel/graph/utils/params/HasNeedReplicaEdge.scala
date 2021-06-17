package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasNeedReplicaEdge extends Params{
  /**
    * Param for flag of replica edges.
    *
    * @group param
    */
  final val needReplicaEdge = new BooleanParam(this, "needReplicaEdge", "need replica edge or not")

  final def getNeedReplicaEdge : Boolean = $(needReplicaEdge)

  setDefault(needReplicaEdge, false)
  
  final def setNeedReplicaEdge(bool: Boolean): this.type = set(needReplicaEdge, bool)
}
