package com.tencent.angel.ml.core.data

import com.tencent.angel.ml.math2.vector.Vector

import scala.beans.BeanProperty


class Example(@BeanProperty var x: Vector, @BeanProperty var y: Double, @BeanProperty var attach: String)

object Example {
  def apply(x: Vector, y: Double, attach: String): Example = new Example(x, y, attach)
}
