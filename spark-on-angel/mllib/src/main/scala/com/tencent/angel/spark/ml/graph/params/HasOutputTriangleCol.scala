package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{Param, Params}

trait HasOutputTriangleCol extends Params {

  final val outputTriangleCol = new Param[String](this, "outputTriangleCol",
    "name for triangle count column")

  final def getOutputTriangleCol: String = ${outputTriangleCol}

  setDefault(outputTriangleCol, "triangleCount")

  final def setOutputTriangleCol(name: String): this.type = set(outputTriangleCol, name)

}
