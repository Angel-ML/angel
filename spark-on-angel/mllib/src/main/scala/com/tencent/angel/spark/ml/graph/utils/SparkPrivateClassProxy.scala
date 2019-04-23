package org.apache.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.util.CollectionsUtils
import org.apache.spark.util.collection.{OpenHashMap, OpenHashSet}
import org.apache.spark.util.random.XORShiftRandom

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object SparkPrivateClassProxy {

  def makeBinarySearch[K: Ordering : ClassTag]: (Array[K], K) => Int = {
    CollectionsUtils.makeBinarySearch
  }

  def sketch[K: ClassTag](rdd: RDD[K],
                          sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    RangePartitioner.sketch(rdd, sampleSizePerPartition)
  }

  def determineBounds[K: Ordering : ClassTag](
                                               candidates: ArrayBuffer[(K, Float)],
                                               partitions: Int): Array[K] = {
    RangePartitioner.determineBounds(candidates, partitions)
  }

  def getXORShiftRandom(seed: Long): XORShiftRandom = new XORShiftRandom(seed)

  def createOpenHashMap[K: ClassTag, V: ClassTag](): OpenHashMap[K, V] = {
    new OpenHashMap[K, V]()
  }

  def createOpenHashSet[K: ClassTag](): OpenHashSet[K] = {
    new OpenHashSet[K]()
  }
}
