package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasPullBatchSize extends Params {
  /**
    * Param for batch size.
    *
    * @group param
    */
  final val pullBatchSize = new IntParam(this, "pullBatchSize", "pullBatchSize")

  /** @group getParam */
  final def getPullBatchSize: Int = $(pullBatchSize)

  setDefault(pullBatchSize, 10000)

  /** @group setParam */
  final def setPullBatchSize(size: Int): this.type = set(pullBatchSize, size)
}
