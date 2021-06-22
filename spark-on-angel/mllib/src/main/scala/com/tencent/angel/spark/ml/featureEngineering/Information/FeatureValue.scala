package com.tencent.angel.spark.ml.featureEngineering.Information

/**
 * Created by allylu on 2016/10/27.
 * 对每个特征下面的特征值进行封装
 */
class FeatureValue extends Serializable {
  // base info
  var id: Int = 0
  var value: String = ""

  // sample info
  var pos: Int = 0
  var neg: Int = 0
  var posneg: Int = 0

  override def toString = {
    var str: StringBuilder = new StringBuilder
    str ++= "feature value info:\n"
    str ++= "id=" + this.id + "\n"
    str ++= "value=" + this.value + "\n"
    str ++= "pos=" + this.pos + "\n"
    str ++= "neg=" + this.neg + "\n"
    str ++= "posneg=" + this.posneg + "\n"

    str.toString
  }

  override def hashCode = {
    var str: StringBuilder = new StringBuilder
    str ++= "id=" + this.id + "\n"
    str ++= "value=" + this.value + "\n"

    str.toString.hashCode
  }

  def canEqual(o: Any): Boolean = o match {
    case f: FeatureValue => true
    case _ => false
  }

  override def equals(o: Any): Boolean = {
    def strictEquals(o: FeatureValue) =
      this.id == o.id && this.value.equals(o.value)

    o match {
      case a: AnyRef if this eq a => true
      case f: FeatureValue => (f canEqual this) && strictEquals(f)
      case _ => false
    }
  }
}