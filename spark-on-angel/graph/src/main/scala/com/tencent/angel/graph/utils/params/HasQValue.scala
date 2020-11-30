package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{DoubleParam, Params}

trait HasQValue extends Params {
  /**
    * Param for Node2Vec.
    *
    * @group param
    */
  final val qValue = new DoubleParam(this, "qValue", "q value od node2vector")

  /** @group getParam */
  final def getQValue: Double = $(qValue)

  final def setQValue(q: Double): this.type = set(qValue, q)
}
