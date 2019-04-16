package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasPSPartitionNum extends Params {
  /**
    * Param for partitionNum.
    *
    * @group param
    */
  final val psPartitionNum = new IntParam(this, "psPartitionNum", "num of partition for ps")

  /** @group getParam */
  final def getPSPartitionNum: Int = $(psPartitionNum)

  final def setPSPartitionNum(num: Int): this.type = set(psPartitionNum, num)
}
