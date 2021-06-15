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
package com.tencent.angel.graph.rank.weightedHIndex

import breeze.linalg.{max, min}
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.ml.math2.VFactory

import scala.collection.mutable


object WeightedHIndexOperator {

  def initMsgs(index: Int, iterator: Iterator[(Long, Array[(Long, Float)])], model: WeightedHIndexPSModel, batchSize: Int): Iterator[Int] = {
    BatchIter(iterator, batchSize).map { iter =>
      val msgs = VFactory.sparseLongKeyFloatVector(model.dim)
      iter.foreach { case (src, neis) =>
        msgs.set(src, neis.map(_._2).sum) // srcNode's degree
      }
      model.updateNodesS(msgs)
      println(s"part $index, init ${msgs.size().toInt} degrees.")
      msgs.clear()
      iter.length
    }
  }


  def process(index: Int, iterator: Iterator[(Long, Array[(Long, Float)])], model: WeightedHIndexPSModel,
              batchSize: Int): Iterator[(Long, Float)] = {
    var startTs = System.currentTimeMillis()
    BatchIter(iterator, batchSize).flatMap { batchIter =>
      println(s"partition $index: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      batchIter.foreach { case (src, neighbors) => pullNodes ++= neighbors.map(_._1) }
      val beforePullTs = System.currentTimeMillis()
      val psDegreeTable = model.readNodesS(pullNodes.toArray)
      println(s"partition $index: process ${batchIter.length} nodes, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      batchIter.map { case (src, neis) =>
        val wei = neis.map(x => (x._2, psDegreeTable.get(x._1))).sortWith(_._2 > _._2)
        (src, calWHIndex(wei))
      }
    }
  }

  def calWHIndex(array: Array[(Float,Float)]): Float ={
    if (array.length < 2) {
      min(array(0)._1, array(0)._2)
    } else {
      var (x, y) = array(0)
      if (x >= y) y
      else {
        var idx = 1
        while (idx < array.length && x < y) {
          x += array(idx)._1
          y = array(idx)._2
          idx += 1
        }
        if (x <= y) x else max(x - array(idx-1)._1 ,y)
      }
    }
  }

}