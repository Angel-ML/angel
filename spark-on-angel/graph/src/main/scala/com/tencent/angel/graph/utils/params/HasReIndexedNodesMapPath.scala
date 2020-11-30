package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasReIndexedNodesMapPath extends Params {

  final val reIndexedNodesMapPath = new Param[String](this, "reIndexedNodesMapPath", "reIndexedNodesMapPath")

  final def getReIndexedNodesMapPath: String = $(reIndexedNodesMapPath)

  setDefault(reIndexedNodesMapPath, "")

  final def setReIndexedNodesMapPath(path: String): this.type = set(reIndexedNodesMapPath, path)

}