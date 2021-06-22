package com.tencent.angel.spark.ml.featureEngineering.Information

import scala.collection.mutable.ArrayBuffer

/**
 * Created by allylu on 2016/10/27.
 * 专门用来封装每个特征的信息，包括id，value
 */
class Feature extends Serializable {

  var id: Int = 0
  val values: ArrayBuffer[FeatureValue] = new ArrayBuffer[FeatureValue]

  override def toString = {
    var str: StringBuilder = new StringBuilder
    str ++= "feature info:\n"
    str ++= "id = " + this.id + "\n"
    str ++= "values = " + this.values + "\n"
    str.toString
  }

  override def hashCode = {
    var str: StringBuilder = new StringBuilder
    str ++= "id=" + this.id + "\n"
    str.toString.hashCode
  }

  def canEqual(o: Any): Boolean = o match {
    case f: Feature => true
    case _ => false
  }

  override def equals(o: Any): Boolean = {
    def strictEquals(o: Feature) =
      this.id == o.id

    o match {
      case a: AnyRef if this eq a => true
      case f: Feature => (f canEqual this) && strictEquals(f)
      case _ => false
    }
  }
}


