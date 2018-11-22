package com.tencent.angel.ml.core.utils

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
      case "notrans" => new NoTrans()
      case "posnegtrans" => new PosNegTrans(threshold)
      case "zeroonetrans" => new ZeroOneTrans(threshold)
      case "addonetrans" => new AddOneTrans()
      case "subonetrans" => new SubOneTrans()
    }
  }
}


