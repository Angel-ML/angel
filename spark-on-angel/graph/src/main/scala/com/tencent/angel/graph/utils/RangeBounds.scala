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

package com.tencent.angel.graph.utils

import java.util.NoSuchElementException

import org.apache.spark.SparkPrivateClassProxy
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.hashing.byteswap32

object RangeBounds {

  def rangeBoundsBySample[K: Ordering : ClassTag](partitions: Int, rdd: RDD[K]): Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(20.0 * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = SparkPrivateClassProxy.sketch(rdd, sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd, imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        SparkPrivateClassProxy.determineBounds(candidates, partitions)
      }
    }
  }

  def rangeBoundsBySort(partition: Int, nodes: RDD[Long]): Array[Long] = {

    val sortedDistinct = nodes.sortBy(x => x)
      .mapPartitions(iter => distinctIter(iter), preservesPartitioning = true)

    val n = sortedDistinct.partitions.length
    val sizes = sortedDistinct.context.runJob(
      sortedDistinct,
      getIteratorSize[Long] _,
      0 until n
    ).scanLeft(0L)(_ + _)

    val step = sizes.last.toDouble / partition
    var quantileIndex = 1
    var cur = (step * quantileIndex).toLong
    val quantileBuffer = new mutable.OpenHashMap[Int, ArrayBuffer[Long]]()
    for (i <- 0 until n) {
      while (sizes(i) <= cur && sizes(i + 1) > cur) {
        quantileBuffer.getOrElseUpdate(i, new ArrayBuffer[Long]()) += (cur - sizes(i))
        quantileIndex += 1
        cur = (step * quantileIndex).toLong
      }
    }
    val bcQuantile = sortedDistinct.context.broadcast(quantileBuffer)

    sortedDistinct.mapPartitionsWithIndex { case (ind, iter) =>
      if (iter.nonEmpty && bcQuantile.value.contains(ind)) {
        val indexes = bcQuantile.value(ind)
        var i = 0
        var j = 0
        new Iterator[Long] {

          override def hasNext: Boolean = i < indexes.length

          override def next(): Long = {
            if (hasNext) {
              while (j != indexes(i)) {
                iter.next()
                j += 1
              }
              j += 1
              i += 1
              iter.next()
            } else {
              throw new NoSuchElementException("next on empty iterator")
            }
          }
        }
      } else {
        Iterator.empty
      }
    }.collect()
  }

  private def getIteratorSize[T](iterator: Iterator[T]): Long = {
    var count = 0L
    while (iterator.hasNext) {
      count += 1L
      iterator.next()
    }
    count
  }

  private def distinctIter(sortedIterator: Iterator[Long]): Iterator[Long] = {
    new Iterator[Long] with Serializable {
      // magic number, 1 for one left, 0 for none left, 2 for other
      var status = 2
      var _cur: Long = sortedIterator.next()
      var _next: Long = {
        var p: Long = _cur
        do {
          p = sortedIterator.next()
        } while (p == _cur && sortedIterator.hasNext)
        if (p == _cur) {
          status = 1
        }
        p
      }
      var _hasNext: Boolean = true

      override def hasNext: Boolean = _hasNext

      override def next(): Long = {
        if (status == 2) {
          val pre = _cur
          _cur = _next
          _next = {
            var p: Long = _cur
            do {
              p = sortedIterator.next()
            } while (p == _cur && sortedIterator.hasNext)
            if (p == _cur) {
              status = 1
            }
            p
          }
          pre
        } else if (status == 1) {
          _hasNext = false
          _cur
        } else {
          throw new NoSuchElementException("next on empty iterator")
        }
      }
    }
  }

  def weightedRangeBounds(partitions: Int, rdd: RDD[(Long, Float)]): Array[Long] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(20.0 * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = sketch(rdd, sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(Long, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, w, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            candidates ++= sample
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd, imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)

          val reSampled =
            new PartitionwiseWeightedSampledRDD[Long, (Long, Float)](imbalanced,
              new NaiveWeightedBernoulliSampler[Long], Map[Int, Double](),
              preservesPartitioning = true, seed).collect()

          candidates ++= reSampled
        }
        determineBounds(candidates, partitions)
      }
    }
  }

  /**
    * Sketches the input RDD via reservoir sampling on each partition.
    *
    * @param rdd                    the input RDD to sketch
    * @param sampleSizePerPartition max sample size per partition
    * @return (total number of items, an array of (partitionId, number of items, sample))
    */
  private def sketch(rdd: RDD[(Long, Float)],
                     sampleSizePerPartition: Int): (Long, Array[(Int, Long, Double, Array[(Long, Float)])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n, w) = reservoirWeightedSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, w, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }

  // A-Res Algorithm
  private def reservoirWeightedSampleAndCount(input: Iterator[(Long, Float)],
                                              k: Int,
                                              seed: Long)
  : (Array[(Long, Float)], Long, Double) = {
    val reservoir = new mutable.PriorityQueue[((Long, Float), Double)]()(
      Ordering.by[((Long, Float), Double), Double](-_._2)
    )
    // Put the first k elements in the reservoir.
    var i = 0
    var s = 0.0
    while (i < k && input.hasNext) {
      val item = input.next()
      val w = math.pow(Random.nextDouble(), 1.0 / item._2)
      s += item._2
      reservoir.enqueue((item, w))
      i += 1
    }

    // If we have consumed all the elements, return them. Otherwise do the replacement.
    if (i < k) {
      // If input size < k, trim the array to return only an array of input size.
      (reservoir.toArray.map(_._1), i, s)
    } else {
      // If input size > k, continue the sampling process.
      var l = i.toLong
      val rand = SparkPrivateClassProxy.getXORShiftRandom(seed)
      while (input.hasNext) {
        val item = input.next()
        l += 1
        val w = math.pow(rand.nextDouble(), 1.0 / item._2)
        s += item._2
        if (reservoir.head._2 > w) {
          reservoir.dequeue()
          reservoir.enqueue((item, w))
        }
      }
      (reservoir.toArray.map(_._1), l, s)
    }
  }

  /**
    * Determines the bounds for range partitioning from candidates with weights indicating how many
    * items each represents. Usually this is 1 over the probability used to sample this candidate.
    *
    * @param candidates unordered candidates with weights
    * @param partitions number of partitions
    * @return selected bounds
    */
  private def determineBounds[K: Ordering : ClassTag](
                                                       candidates: ArrayBuffer[(K, Float)],
                                                       partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }
}
