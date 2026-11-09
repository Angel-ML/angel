package com.tencent.angel.spark.ml.ann

trait DistanceFunction extends Serializable {
  
  def distance(u: Array[Float], v: Array[Float]): Float
  
  def getName: String
}
