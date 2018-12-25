package com.tencent.angel.ml.core.data


import com.tencent.angel.ml.core.utils.JsonUtils.matchClassName

sealed trait TransLabel extends Serializable {
  def trans(label: Double): Double
}

class NoTrans extends TransLabel{
  override def trans(label: Double): Double = label
}

class PosNegTrans(threshold: Double = 0) extends TransLabel{
  override def trans(label: Double): Double = {
    if (label > threshold) 1.0 else - 1.0
  }
}

class ZeroOneTrans(threshold: Double = 0) extends TransLabel{
  override def trans(label: Double): Double = {
    if (label > threshold) 1.0 else 0.0
  }
}

class AddOneTrans extends TransLabel {
  override def trans(label: Double): Double = label + 1.0
}

class SubOneTrans extends TransLabel {
  override def trans(label: Double): Double = label - 1.0
}


object TransLabel {
  def get(name: String, threshold: Double = 0): TransLabel = {
    name.toLowerCase match {
      case name: String if matchClassName[NoTrans](name) => new NoTrans()
      case name: String if matchClassName[PosNegTrans](name) => new PosNegTrans(threshold)
      case name: String if matchClassName[ZeroOneTrans](name) => new ZeroOneTrans(threshold)
      case name: String if matchClassName[AddOneTrans](name) => new AddOneTrans()
      case name: String if matchClassName[SubOneTrans](name) => new SubOneTrans()
    }
  }
}


