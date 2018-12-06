package com.tencent.angel.ml.core.data

trait Block[+T] {
  def resetReadIndex(): Unit

  def loopingRead(): T

  def size(): Long

  def shuffle(): Unit
}
