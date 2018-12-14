package com.tencent.angel.ml.core.utils

import java.util.concurrent.Future

class Callback[T]() {
  private var future: Future[T] = _

  def this(future: Future[T]) = {
    this()
    this.future = future
  }


  def setFuture(future: Future[T]): Unit = {
    this.future = future
  }

  def get(): Unit = {
    if (future != null) {
      future.get
    }
  }
}

object Callback {
  class VoidType

  def voidCallback(): Callback[VoidType] = new Callback[VoidType]()

}
