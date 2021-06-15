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
package com.tencent.angel.graph.rank.hindex

import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.ml.math2.VFactory

import scala.collection.mutable


/**
  * A neighbor table tool for H-Index , Weighted H-Index and G-Index
  *
  */
object HIndexOperator {
  /**
    * init degree of each node
    *
    * @param model
    * @return
    */
  def initMsgs(index: Int, iterator: Iterator[(Long, Array[Long])], model: HIndexPSModel, batchSize: Int): Iterator[Int] = {
    BatchIter(iterator, batchSize).map { iter =>
      val msgs = VFactory.sparseLongKeyIntVector(model.dim)
      iter.foreach { case (src, neis) =>
        msgs.set(src, neis.length) // srcNode's degree
      }
      model.initMsgs(msgs)
      println(s"part $index, init ${msgs.size().toInt} degrees.")
      msgs.clear()
      iter.length
    }
  }

  /**
    *
    * @param model
    * @return
    */
  def process(index: Int, iterator: Iterator[(Long, Array[Long])], model: HIndexPSModel,
              batchSize: Int): Iterator[(Long, Int, Int, Int)] = {
    var startTs = System.currentTimeMillis()
    BatchIter(iterator, batchSize).flatMap { batchIter =>
      println(s"partition $index: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      batchIter.foreach { case (src, neighbors) => pullNodes ++= neighbors }
      val beforePullTs = System.currentTimeMillis()
      val psDegreeTable = model.readMsgs(pullNodes.toArray)
      println(s"partition $index: process ${batchIter.length} nodes, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      batchIter.map { case (src, neis) =>
        val hIndex = neis.map(x => psDegreeTable.get(x)).sorted
        (src, calcHIndex(hIndex), calcGIndex(hIndex), calcWIndex(hIndex))
      }
    }
  }

  def calcHIndex(hIndex: Array[Int]): Int = {
    var i = hIndex.length - 1
    var cnt = 1
    //compute h-index
    while (i >= 0 && hIndex(i) >= cnt) {
      cnt += 1
      i -= 1
    }

    cnt - 1
  }

  def calcGIndex(hIndex: Array[Int]): Int = {
    val end = hIndex.length
    var i = end - 1
    var cnt = 0
    var g2 = 0
    var gIndex = 0
    while (i >= 0 && cnt >= g2) {
      gIndex += 1
      cnt += hIndex(i)
      g2 = (end - i) * (end - i)
      i -= 1
    }
    if (cnt >= g2) {
      gIndex
    } else {
      gIndex - 1
    }
  }

  def calcWIndex(hIndex: Array[Int]): Int = {
    var i = hIndex.length - 1
    var cnt = 1
    var j = 1
    while (i >= 0 && hIndex(i) >= 10 * cnt) {
      if (j >= cnt) {
        j += 1
        cnt += 1
      }
      i -= 1
    }
    cnt - 1
  }

}