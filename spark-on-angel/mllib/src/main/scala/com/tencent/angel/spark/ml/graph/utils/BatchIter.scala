package com.tencent.angel.spark.ml.graph.utils

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object BatchIter {
  def apply[T: ClassTag](iterator: Iterator[T], batchSize: Int): Iterator[Array[T]] = {
    new Iterator[Array[T]] {
      val buffer = new ArrayBuffer[T]()

      override def hasNext: Boolean = iterator.hasNext

      override def next(): Array[T] = {
        buffer.clear()
        var num = 0
        while (num < batchSize && iterator.hasNext) {
          num += 1
          buffer += iterator.next()
        }
        buffer.toArray
      }
    }
  }
}
