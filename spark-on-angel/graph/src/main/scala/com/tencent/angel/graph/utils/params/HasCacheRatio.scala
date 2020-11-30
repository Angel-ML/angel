package com.tencent.angel.graph.utils.params

import org.apache.spark.ml.param.{DoubleParam, Params}

trait HasCacheRatio extends Params {
  final val cacheRatio = new DoubleParam(this, "CacheRatio", "Cache Ratio")

  final def getCacheRatio: Double = $(cacheRatio)

  final def setCacheRatio(hr: Double): this.type = set(cacheRatio, hr)
}
