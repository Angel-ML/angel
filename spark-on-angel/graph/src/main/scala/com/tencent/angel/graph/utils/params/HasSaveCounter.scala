package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{Param, Params}

trait HasSaveCounter extends Params {
  /**
   * Param for isSaveCounter.
   *
   * @group param
   */
  final val isSaveCounter = new Param[Boolean](this, "isSaveCounter",
    "isSaveCounter")

  /** @group getParam */
  final def getSaveCounter: Boolean = $(isSaveCounter)

  setDefault(isSaveCounter, false)

  /** @group setParam */
  def setSaveCounter(name: Boolean): this.type = set(isSaveCounter, name)
}
