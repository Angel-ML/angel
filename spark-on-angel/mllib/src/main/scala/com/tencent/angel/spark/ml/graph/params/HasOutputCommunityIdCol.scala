package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{Param, Params}

trait HasOutputCommunityIdCol extends Params {

  /**
    * Param for name of output community id.
    *
    * @group param
    */
  final val outputCommunityIdCol = new Param[String](this, "outputCommunityIdCol", "name for output community id column")

  /** @group getParam */
  final def getOutputCommunityIdCol: String = $(outputCommunityIdCol)

  setDefault(outputCommunityIdCol, "comm")

  /** @group setParam */
  final def setOutputCommunityIdCol(name: String): this.type = set(outputCommunityIdCol, name)

}
