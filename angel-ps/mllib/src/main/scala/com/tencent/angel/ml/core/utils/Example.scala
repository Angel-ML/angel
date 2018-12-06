package com.tencent.angel.ml.core.utils

import com.tencent.angel.ml.math2.vector.Vector

abstract class Example {
  def getX: Vector

  def setX(x: Vector): Unit

  def getY: Double

  def setY(y: Double): Unit

  def getAttach: String

  def attach(msg: String): Unit
}
