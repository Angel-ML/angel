package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{Param, Params}

trait HasDstNodeIdCol extends Params {
  /**
    * Param for name of dst node id.
    *
    * @group param
    */
  final val dstNodeIdCol = new Param[String](this, "dstNodeIdCol", "name for dst id column")

  /** @group getParam */
  final def getDstNodeIdCol: String = $(dstNodeIdCol)

  setDefault(dstNodeIdCol, "dst")

  /** @group setParam */
  final def setDstNodeIdCol(name: String): this.type = set(dstNodeIdCol, name)
}
