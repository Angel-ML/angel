package org.apache.spark.util.collection

import scala.reflect.ClassTag

object SparkCollectionProxy {
  def createOpenHashMap[K: ClassTag, V: ClassTag](): OpenHashMap[K, V] = {
    new OpenHashMap[K, V]()
  }
}
