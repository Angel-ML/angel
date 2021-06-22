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

package com.tencent.angel.graph.embedding.word2vec

import java.util.{HashMap => JHashMap, HashSet => JHashSet}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

object Word2VecUtils {

  def corpusStringToInt(data: RDD[String], minCount: Int): (RDD[Array[Int]], RDD[(Int, String)]) = {

    // All distinct strings, filter min count words
    val strings = data.filter(f => f != null && f.length > 0)
      .map(f => f.stripLineEnd.split("[\\s+|,]")).flatMap(f => f)
      .map(t => (t, 1)).reduceByKey(_ + _).filter(ele => ele._2 > minCount).map(f => f._1)

    val stringsWithIndex = strings.zipWithIndex().map(ele => (ele._1, ele._2 + 1)).cache()


    def buildRoutingTable(index: Int, iterator: Iterator[String]): Iterator[(String, Int)] = {
      val set = new JHashSet[String]()
      while (iterator.hasNext) {
        val line = iterator.next()
        if (line != null && line.length > 0) {
          for (word <- line.stripLineEnd.split("[\\s+|,]")) {
            set.add(word)
          }
        }
      }

      val it = set.iterator()
      val result = new ArrayBuffer[(String, Int)]()
      while (it.hasNext) {
        result.append((it.next(), index))
      }

      result.iterator
    }

    val routingTable = data.mapPartitionsWithIndex((partId, iterator) =>
      buildRoutingTable(partId, iterator),
      true)

    val partIndex = routingTable.join(stringsWithIndex).map { case (str, (partId, index)) =>
      (partId, (str, index))
    }.groupByKey(data.getNumPartitions)

    def attachPartitionId(index: Int, iterator: Iterator[String]): Iterator[(Int, Array[String])] = {
      Iterator.single((index, iterator.toArray))
    }

    val ints = data.mapPartitionsWithIndex((partId, iterator) =>
      attachPartitionId(partId, iterator),
      true).join(partIndex).map { case (_, (sentences, mapping)) =>
      val map = new JHashMap[String, Long]()
      for ((string, index) <- mapping) {
        map.put(string, index)
      }

      sentences.filter(f => f != null && f.length > 0).map(line =>
        line.stripLineEnd.split("[\\s+|,]").map(s => map.get(s).toInt).filter(ele => ele > 0))
    }.flatMap(f => f)


    (ints, stringsWithIndex.map(f => (f._2.toInt, f._1)))
  }

  def corpusStringToIntWithoutRemapping(data: RDD[String], minCount: Int): RDD[Array[Int]] = {
    if (minCount > 0) {
      // All distinct strings, filter min count words
      val strings = data.filter(f => f != null && f.length > 0)
        .map(f => f.stripLineEnd.split("[\\s+|,]")).flatMap(f => f)
        .map(t => (t, 1)).reduceByKey(_ + _).filter(ele => ele._2 > minCount).map(f => f._1)

      val stringsWithIndex = strings.zipWithIndex().map(ele => (ele._1, ele._2 + 1)).cache()


      def buildRoutingTable(index: Int, iterator: Iterator[String]): Iterator[(String, Int)] = {
        val set = new JHashSet[String]()
        while (iterator.hasNext) {
          val line = iterator.next()
          if (line != null && line.length > 0) {
            for (word <- line.stripLineEnd.split("[\\s+|,]")) {
              set.add(word)
            }
          }
        }

        val it = set.iterator()
        val result = new ArrayBuffer[(String, Int)]()
        while (it.hasNext) {
          result.append((it.next(), index))
        }

        result.iterator
      }

      val routingTable = data.mapPartitionsWithIndex((partId, iterator) =>
        buildRoutingTable(partId, iterator),
        true)

      val partIndex = routingTable.join(stringsWithIndex).map { case (str, (partId, index)) =>
        (partId, (str, index))
      }.groupByKey(data.getNumPartitions)

      def attachPartitionId(index: Int, iterator: Iterator[String]): Iterator[(Int, Array[String])] = {
        Iterator.single((index, iterator.toArray))
      }

      val ints = data.mapPartitionsWithIndex((partId, iterator) =>
        attachPartitionId(partId, iterator),
        true).join(partIndex).map { case (_, (sentences, mapping)) =>
        val map = new JHashMap[String, Long]()
        for ((string, index) <- mapping) {
          map.put(string, index)
        }

        sentences.filter(f => f != null && f.length > 0).map(line =>
          line.stripLineEnd.split("[\\s+|,]").filter(s => map.get(s).toInt > 0).map(ele => ele.toInt))
      }.flatMap(f => f)
      ints
    } else {
      data.filter(f => f != null && f.length > 0)
        .map(f => f.stripLineEnd.split("[\\s+|,]").map(s => s.toInt))
    }
  }

  def parseBatchData(sentences: Array[Array[Int]], windowSize: Int): (Array[Int], Array[Int]) = {
    val srcNodes = new ArrayBuffer[Int]()
    val dstNodes = new ArrayBuffer[Int]()
    for (s <- sentences.indices) {
      val sen = sentences(s)
      for (srcIndex <- sen.indices) {
        var dstIndex = Math.max(srcIndex - windowSize, 0)
        while (dstIndex < Math.min(srcIndex + windowSize + 1, sen.length)) {
          if (srcIndex != dstIndex) {
            srcNodes.append(sen(dstIndex))
            dstNodes.append(sen(srcIndex))
          }
          dstIndex += 1
        }
      }
    }
    (srcNodes.toArray, dstNodes.toArray)
  }

  def summarizeReduceOp(t1: (Int, Int, Int, Long, Long),
                        t2: (Int, Int, Int, Long, Long)): (Int, Int, Int, Long, Long) =
    (math.min(t1._1, t2._1), math.max(t1._2, t2._2), math.max(t1._3, t2._3) ,t1._4 + t2._4, t1._5 + t2._5)

  def summarizeApplyOp(iterator: Iterator[Array[Int]]): Iterator[(Int, Int, Int, Long, Long)] = {
    var minId = Int.MaxValue
    var maxId = Int.MinValue
    var numDocs = 0L
    var numTokens = 0L
    var maxLength = Int.MinValue
    while (iterator.hasNext) {
      val entry = iterator.next()
      minId = math.min(minId, entry.min)
      maxId = math.max(maxId, entry.max)
      maxLength = math.max(maxLength, entry.length)
      numDocs += 1
      numTokens += entry.length
    }
    Iterator.single((minId, maxId, maxLength, numDocs, numTokens))
  }
}
