package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasCheckPointInterval extends Params {
  /**
    * Param for buffer size.
    *
    * @group param
    */
  final val checkpointInterval = new IntParam(this, "checkpointInterval", "checkpointInterval")

  /** @group getParam */
  final def getCheckpointInterval: Int = $(checkpointInterval)

  setDefault(checkpointInterval, 5)

  /** @group setParam */
  final def setCheckpointInterval(interval: Int): this.type = set(checkpointInterval, interval)
}
