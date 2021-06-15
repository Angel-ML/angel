package com.tencent.angel.graph.model.ops

import com.tencent.angel.ml.matrix.MatrixContext

trait CommonOps {
  def init(): Unit = init(new MatrixContext())

  def init(mc: MatrixContext): Unit

  def start(): Unit = {}

  def end(): Unit = {}

  def checkpoint(): Unit
}
