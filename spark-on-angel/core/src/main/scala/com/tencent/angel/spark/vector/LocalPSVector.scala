package com.tencent.angel.spark.vector

import com.tencent.angel.spark.{PSClient, PSVector, PSVectorProxy}

/**
 * LocalPSVector is the local form of PSVector.
 * `get` PSVector to local is the only operation.
 */
class LocalPSVector private[spark](override val proxy: PSVectorProxy)
  extends PSVector {

  @transient
  @volatile private var value: Array[Double] = _

  def get(): Array[Double] = {
    if (value == null) {
      this.synchronized {
        if (value == null) {
          value = PSClient.get.get(proxy)
        }
      }
    }
    value
  }

}
