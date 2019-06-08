package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasPartitionNum extends Params {
  /**
    * Param for partitionNum.
    *
    * @group param
    */
  final val partitionNum = new IntParam(this, "partitionNum", "num of partition for rdd")

  /** @group getParam */
  final def getPartitionNum: Int = $(partitionNum)

  final def setPartitionNum(num: Int): this.type = set(partitionNum, num)

}
