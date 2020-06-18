package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasSrcNodeLccCol extends Params {
  /**
    * Param for name of src node lcc.
    *
    * @group param
    */
  final val srcNodeLccCol = new Param[String](this, "srcNodeLccCol", "name for src lcc column")

  /** @group getParam */
  final def getSrcNodeLccCol: String = $(srcNodeLccCol)

  setDefault(srcNodeLccCol, "lcc")

  /** @group setParam */
  final def setSrcNodeLccCol(name: String): this.type = set(srcNodeLccCol, name)
}
