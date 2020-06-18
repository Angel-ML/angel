package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{IntParam, Params}

trait HasModelSaveInterval extends Params {
  /**
    * Param for buffer size.
    *
    * @group param
    */
  final val saveModelInterval = new IntParam(this, "saveModelInterval", "saveModelInterval")

  /** @group getParam */
  final def getSaveModelInterval: Int = $(saveModelInterval)

  setDefault(saveModelInterval, 5)

  /** @group setParam */
  final def setSaveModelInterval(interval: Int): this.type = set(saveModelInterval, interval)
}
