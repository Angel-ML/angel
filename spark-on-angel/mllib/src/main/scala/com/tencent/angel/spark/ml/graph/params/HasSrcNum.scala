package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasSrcNum extends Params {

  /**
    * Param for partitionNum.
    *
    * @group param
    */
  final val srcNum = new IntParam(this, "srcNum",
    "num of sample sources (k) for Closeness algorithm")

  /** @group getParam */
  final def getSrcNum: Int = $(srcNum)

  setDefault(srcNum, 20)

  /** @group setParam */
  final def setSrcNum(num: Int): this.type = set(srcNum, num)

}
