/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.spark.ml.util

import com.tencent.angel.ml.math2.storage.LongKeyVectorStorage
import com.tencent.angel.ml.matrix.{MatrixContext, PartContext}
import com.tencent.angel.ml.math2.vector.{LongDummyVector, LongKeyVector, Vector}
import org.apache.spark.rdd.RDD

/**
  * Automatically generate model partitions according to the distribution of feature index
  *
  * @param numPartitions , the number of model partitions
  */
abstract class AutoPartitioner(val bits: Int, numPartitions: Int) extends Serializable {

  def getBuckets(data: RDD[Vector]): Array[(Long, Long)]

  def getBucketsIndex(data: RDD[Long]): Array[(Long, Long)]

  /**
    * Generate partitions from data into ctx.
    *
    * @param data , the training data
    * @param ctx  , the matrix context for the model
    */
  def partition(data: RDD[Vector], ctx: MatrixContext): Unit = {
    val buckets = getBuckets(data)
    partition(buckets, ctx)
  }

  def partitionIndex(data: RDD[Long], ctx: MatrixContext): Unit = {
    val buckets = getBucketsIndex(data)
    partition(buckets, ctx)
  }

  def partition(buckets: Array[(Long, Long)], ctx: MatrixContext): Unit = {
    //    println(s"bucket.size=${buckets.size}")
    val sorted = buckets.sortBy(f => f._1)
    val sum = sorted.map(f => f._2).sum
    val per = sum / numPartitions

    var start = ctx.getIndexStart
    val end = ctx.getIndexEnd
    val rowNum = ctx.getRowNum

    var current = 0L
    val size = sorted.length
    val limit = ((end.toDouble - start.toDouble) / numPartitions).toLong * 4
    println(limit)
    for (i <- 0 until size) {
      // keep each partition similar load and limit the range of each partition
      if (current > per || ((sorted(i)._1 << bits - start > limit) && current > per / 2)) {
        val part = new PartContext(0, rowNum, start, sorted(i)._1 << bits, 0)
        println(s"part=$part load=$current range=${part.getEndCol - part.getStartCol}")
        ctx.addPart(part)
        start = sorted(i)._1 << bits
        current = 0L
      }
      current += sorted(i)._2
    }

    val part = new PartContext(0, rowNum, start, end, 0)
    ctx.addPart(part)
    println(s"part=$part load=$current range=${end - start}")
    println(s"split matrix ${ctx.getName} into ${ctx.getParts.size()} partitions")
  }
}

/**
  * Generate buckets according to the number of elements for each feature
  *
  * @param bits          , use how many bits (lower) to generate a bucket
  * @param numPartitions , the number of model partitions
  */
class LoadBalancePartitioner(bits: Int = -1, numPartitions: Int)
  extends AutoPartitioner(bits, numPartitions) {

  override def getBuckets(data: RDD[Vector]): Array[(Long, Long)] = {
    val indices = data.flatMap {
      case dummy: LongDummyVector => dummy.getIndices
      case longKey: LongKeyVector => longKey.getStorage.asInstanceOf[LongKeyVectorStorage]
        .getIndices
    }

    val buckets = indices.map(f => f / (1L << bits))
      .map(f => (f, 1L)).reduceByKey(_ + _).sortBy(_._1).collect()
    buckets
  }

  override def getBucketsIndex(data: RDD[Long]): Array[(Long, Long)] = {
    val buckets = data.map(f => f / (1L << bits))
      .map(f => (f, 1L)).reduceByKey(_ + _).sortBy(_._1).collect()
    buckets
  }
}

/**
  * Generate buckets according to the number of features
  *
  * @param bits          , use how many bits (lower) to generate a bucket
  * @param numPartitions , the number of model partitions
  */
class StorageBalancePartitioner(bits: Int, numPartitions: Int)
  extends AutoPartitioner(bits, numPartitions) {
  override def getBuckets(data: RDD[Vector]): Array[(Long, Long)] = {
    val indices = data.flatMap {
      case dummy: LongDummyVector => dummy.getIndices
      case longKey: LongKeyVector => longKey.getStorage.asInstanceOf[LongKeyVectorStorage]
        .getIndices
    }

    val buckets = indices.distinct().map(f => f / (1L << bits))
      .map(f => (f, 1L)).reduceByKey(_ + _).sortBy(_._1).collect()
    buckets
  }

  override def getBucketsIndex(data: RDD[Long]): Array[(Long, Long)] = {
    val bucket = data.distinct().map(f => f / (1L << bits))
      .map(f => (f, 1L)).reduceByKey(_ + _).sortBy(_._1).collect()
    bucket
  }
}


object LoadBalancePartitioner {

  def getBuckets(data: RDD[Long], bits: Int): Array[(Long, Long)] = {
    val buckets = data.map(f => f / (1L << bits))
      .map(f => (f, 1L)).reduceByKey(_ + _, 100).sortBy(_._1).collect()
    buckets
  }

  def partition(buckets: Array[(Long, Long)], ctx: MatrixContext, bits: Int, numPartitions: Int): Unit = {
    println(s"bucket.size=${buckets.length}")
    val sorted = buckets.sortBy(f => f._1)
    val sum = sorted.map(f => f._2).sum
    val per = sum / numPartitions

    var start = ctx.getIndexStart
    val end = ctx.getIndexEnd
    val rowNum = ctx.getRowNum

    var current = 0L
    val size = sorted.length
    val limit = ((end.toDouble - start.toDouble) / numPartitions).toLong * 4
    println(s"start: $start, end: $end, limit: $limit")
    for (i <- 0 until size) {
      // keep each partition similar load and limit the range of each partition
      val range = sorted(i)._1 << bits - start
      if ((current > per)
        || ((range > limit) && (current > (per / 2)))){
        val part = new PartContext(0, rowNum, start, sorted(i)._1 << bits, 0)
        println(s"part=$part load=$current range=$range per=$per limit=$limit")
        ctx.addPart(part)
        start = sorted(i)._1 << bits
        current = 0L
      }
      current += sorted(i)._2
    }

    val part = new PartContext(0, rowNum, start, end, 0)
    ctx.addPart(part)
    println(s"part=${part} load=${current} range=${end - start}")
    println(s"split matrix ${ctx.getName} into ${ctx.getParts.size()} partitions")
  }

  def partition(index: RDD[Long], maxId: Long, psPartitionNum: Int, ctx: MatrixContext, percent: Float = 0.7f, minId: Long = 0L): Unit = {
    //    var percent = 0.3
    var p = percent
    val maxIdBits = numBits(maxId - minId)
    val maxNumForEachBucket = (index.count * 0.1).toInt
    while (1L << (maxIdBits * p).toInt > maxNumForEachBucket) {
      println(s"${1L << (maxIdBits * p).toInt} vs $maxNumForEachBucket, num of keys for each bucket exceeds max number, decrease p by 0.05.")
      p -= 0.05f
    }

    val bits = (numBits(maxId - minId) * p).toInt
    println(s"bits used for load balance partition is $bits")
    val buckets = getBuckets(index, bits)
    val sum = buckets.map(f => f._2).sum
    val max = buckets.map(f => f._2).max
    val size = buckets.size
    println(s"max=$max sum=$sum size=$size")
    //    assert(max.toDouble / sum < 0.1, s"max num of loads exceeds 10% of sum: $max vs $sum")
    partition(buckets, ctx, bits, psPartitionNum)
  }

  def partition(index: RDD[Long], psPartitionNum: Int, ctx:MatrixContext): Unit = {
    val sortedIndex = index.map(x => (x, 1)).sortByKey(true, psPartitionNum).map(x => x._1)
    val ctxPartRDD = sortedIndex.mapPartitions{iter =>
      var minId = Long.MaxValue
      var maxId = Long.MinValue
      var num = 0
      while(iter.hasNext) {
        val iterValue = iter.next()
        if (iterValue < minId) {
          minId = iterValue
        } else if (iterValue > maxId) {
          maxId = iterValue
        }
        num += 1
      }
      val part = new PartContext(0, ctx.getRowNum, minId, maxId+1, 0)
      println(s"part=$part startIndex=$minId endIndex=${maxId+1} count=$num")
      Iterator.single(part)
    }
    val  parts = ctxPartRDD.collect()
    parts.foreach(part => ctx.addPart(part))
    val partIter = ctx.getParts.iterator()
    while(partIter.hasNext) {
      val part = partIter.next()
      println(s"part=$part range=${part.getEndCol-part.getStartCol}")
    }
    println(s"split matrix ${ctx.getName} into ${ctx.getParts.size()} partitions")
  }


  def numBits(maxId: Long): Int = {
    var num = 0
    var value = maxId
    while (value > 0) {
      value >>= 1
      num += 1
    }
    num
  }
}