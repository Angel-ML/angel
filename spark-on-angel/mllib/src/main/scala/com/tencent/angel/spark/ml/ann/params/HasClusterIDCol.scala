package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{Param, Params}

trait HasClusterIDCol extends Params {
  /**
    * Param for name of ClusterID.
    *
    * @group param
    */
  final val clusterIDCol = new Param[String](this, "clusterIDCol", "name for vector column")

  /** @group getParam */
  final def getClusterIDCol: String = $(clusterIDCol)

  setDefault(clusterIDCol, "clusterID")

  /** @group setParam */
  final def setClusterIDCol(name: String): this.type = set(clusterIDCol, name)
}