package com.tencent.angel.spark.ml.graph.params

import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.storage.StorageLevel

trait HasStorageLevel extends Params {
  /**
    * Param for storage level.
    *
    * @group param
    */
  final val storageLevel = new Param[StorageLevel](this, "storageLevel", "storage level for persist rdd")

  /** @group getParam */
  final def getStorageLevel: StorageLevel = $(storageLevel)

  setDefault(storageLevel, StorageLevel.DISK_ONLY)

  /** @group setParam */
  final def setStorageLevel(level: String): this.type = set(storageLevel, StorageLevel.fromString(level))

  /** @group setParam */
  final def setStorageLevel(level: StorageLevel): this.type = set(storageLevel, level)
}
