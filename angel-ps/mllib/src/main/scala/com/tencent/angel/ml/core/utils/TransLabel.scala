package com.tencent.angel.ml.core.utils

import scala.reflect.ClassTag

sealed trait TransLabel extends Serializable {
  def trans(label: Double): Double
}

class NoTrans extends TransLabel{
  override def trans(label: Double): Double = label
}

class PosNegTrans(threshold: Double = 0.5) extends TransLabel{
  override def trans(label: Double): Double = {
    if (label > threshold) 1.0 else - 1.0
  }
}

class ZeroOneTrans(threshold: Double = 0.5) extends TransLabel{
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
  private def matchClass[T: ClassTag](name: String): Boolean = {
    val clz = implicitly[ClassTag[T]].runtimeClass
    clz.getSimpleName.equalsIgnoreCase(name)
  }

  def get(name: String, threshold: Double = 0): TransLabel = {
    name match {
      case clzName if matchClass[NoTrans](clzName) => new NoTrans()
      case clzName if matchClass[PosNegTrans](clzName) => new PosNegTrans(threshold)
      case clzName if matchClass[ZeroOneTrans](clzName) => new ZeroOneTrans(threshold)
      case clzName if matchClass[AddOneTrans](clzName) => new AddOneTrans()
      case clzName if matchClass[SubOneTrans](clzName) => new SubOneTrans()
    }
  }
}


