package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{Param, Params}

trait HasOutputDistanceCol extends Params {

  final val outputDistanceCol = new Param[String](this, "outputDistanceCol",
    "name for distance column on sssp algorithm")

  final def getOutputDistanceCol: String = ${outputDistanceCol}

  setDefault(outputDistanceCol, "distance")

  final def setOutputDistanceCol(name: String): this.type = set(outputDistanceCol, name)

}
