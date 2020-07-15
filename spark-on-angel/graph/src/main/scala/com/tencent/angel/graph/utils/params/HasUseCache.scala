package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{BooleanParam, Params}

trait HasUseCache extends Params {
  /**
    * Param for isCompressed.
    *
    * @group param
    */
  final val useCache = new BooleanParam(this, "useCache", "use cache or not")

  final def getUseCache: Boolean = $(useCache)

  final def setUseCache(bool: Boolean): this.type = set(useCache, bool)
}
