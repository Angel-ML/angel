package com.tencent.angel.spark.ml.ann

class Item(val id: Long,
           val vector: Array[Float]) extends Serializable{
  
  def getId: Long = this.id
  
  def getVector: Array[Float] = this.vector
  
  def getDimensions: Int = this.vector.length
}
