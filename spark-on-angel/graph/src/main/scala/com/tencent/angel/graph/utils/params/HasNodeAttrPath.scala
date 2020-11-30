package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasNodeAttrPath extends Params {

  final val nodeAttrPath = new Param[String](this, "nodeAttrPath", "nodeAttrPath")

  final def getNodeAttrPath: String = $(nodeAttrPath)

  setDefault(nodeAttrPath, "")

  final def setNodeAttrPath(path: String): this.type = set(nodeAttrPath, path)

}