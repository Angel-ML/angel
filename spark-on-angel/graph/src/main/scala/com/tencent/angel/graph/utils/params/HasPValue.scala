package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{DoubleParam, ParamMap, Params}


trait HasPValue extends Params {
  /**
    * Param for Node2Vec.
    *
    * @group param
    */
  final val pValue = new DoubleParam(this, "pValue", "p value od node2vector")

  /** @group getParam */
  final def getPValue: Double = $(pValue)

  final def setPValue(p: Double): this.type = set(pValue, p)
}
