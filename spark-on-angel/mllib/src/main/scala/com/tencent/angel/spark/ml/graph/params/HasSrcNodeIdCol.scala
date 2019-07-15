package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{Param, Params}

trait HasSrcNodeIdCol extends Params {
  /**
    * Param for name of src node id.
    *
    * @group param
    */
  final val srcNodeIdCol = new Param[String](this, "srcNodeIdCol", "name for src id column")

  /** @group getParam */
  final def getSrcNodeIdCol: String = $(srcNodeIdCol)

  setDefault(srcNodeIdCol, "src")

  /** @group setParam */
  final def setSrcNodeIdCol(name: String): this.type = set(srcNodeIdCol, name)
}
