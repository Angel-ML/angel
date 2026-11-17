package com.tencent.angel.spark.ml.ann.index

import com.tencent.angel.spark.ml.ann.{DistanceFunction, DistanceFunctions}

trait Index {
  var distanceFunction: DistanceFunction = new DistanceFunctions.FloatL2Distance
  var distanceComparator: Ordering[Float] = Ordering.fromLessThan[Float]((a, b) => a < b)
  def setDistanceFunc(distFunc: DistanceFunction): Unit = {
    this.distanceFunction = distFunc
  }
  def setDistanceComparator(distComparator: Ordering[Float]): Unit = {
    this.distanceComparator = distComparator
  }
}
