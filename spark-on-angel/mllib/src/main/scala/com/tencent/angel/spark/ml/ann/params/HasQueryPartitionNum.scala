package com.tencent.angel.spark.ml.ann.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasQueryPartitionNum extends Params {
  /**
    * Param for partition num of query rdd.
    *
    * @group param
    */
  final val queryPartitionNum = new IntParam(this, "queryPartitionNum", "param for queryPartitionNum")
  
  /** @group getParam */
  final def getQueryPartitionNum: Int = $(queryPartitionNum)
  
  setDefault(queryPartitionNum, 10)
  
  /** @group setParam */
  final def setQueryPartitionNum(m: Int): this.type = set(queryPartitionNum, m)
}