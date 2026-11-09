package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{Param, Params}

trait HasQueryPath extends Params {
  /**
    * Param for path of queries.
    *
    * @group param
    */
  final val queryPath = new Param[String](this, "queryPath", "name for query path")
  
  /** @group getParam */
  final def getQueryPath: String = $(queryPath)
  
  setDefault(queryPath, null)
  
  /** @group setParam */
  final def setQueryPath(name: String): this.type = set(queryPath, name)
}