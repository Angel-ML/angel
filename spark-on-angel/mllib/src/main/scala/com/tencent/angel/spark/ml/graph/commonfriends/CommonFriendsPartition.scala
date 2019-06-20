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

package com.tencent.angel.spark.ml.graph.commonfriends

class CommonFriendsPartition(val srcNodes: Array[Int],
                             val adjNodes: Array[Array[Int]]) extends Serializable {

  assert(srcNodes.length == adjNodes.length)

  lazy val numSrcNodes: Int = {
    val num = srcNodes.length
    num
  }

  lazy val startSrcNodeId: Int = srcNodes(0)

  lazy val endSrcNodeId: Int = srcNodes(srcNodes.length - 1)

  def hasLocal(nodeId: Int): Boolean = nodeId >= startSrcNodeId && nodeId <= endSrcNodeId

  def getLocalNeighbor(nodeId: Int): Option[Array[Int]] = {
    if (hasLocal(nodeId)) Some(adjNodes(nodeId - startSrcNodeId)) else None
  }

  def getSrcNode(idx: Int): Int = srcNodes(idx)

  def neighborsOfNode(idx: Int): Array[Int] = {
    adjNodes(idx)
  }

  def degreeOfNode(idx: Int): Int = {
    adjNodes(idx).length
  }

  def totalDegree(): Int = {
    adjNodes.map(_.length).sum
  }

  def avgDegree(): Int = {
    totalDegree() / numSrcNodes
  }

  def estimatedSizeInByte(): Int = {
    val systemVersion = System.getProperty("sun.arch.data.model")
    val numInts = srcNodes.length + adjNodes.map(_.length).sum
    4 * numInts
  }

  def estimatedSizeInGB(): Double = {
    estimatedSizeInByte() / 1024.0 / 1024.0
  }

  def makeBatchIterator(batchSize: Int): Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    var index = 0

    override def next(): (Int, Int) = {
      val preIndex = index
      index = index + batchSize
      (preIndex, math.min(index, srcNodes.length))
    }

    override def hasNext: Boolean = {
      index < srcNodes.length
    }
  }

  def runEachPartition(psModel: CommonFriendsPSModel): Iterator[((Int, Int), Int)] = {
    var curTime = System.currentTimeMillis()
    println(s"num of src nodes: $numSrcNodes")
    val sortedSrcNodes = srcNodes.sorted
    println(s"start of node: ${sortedSrcNodes(0)}, end of node: ${sortedSrcNodes(sortedSrcNodes.length -1)}")
    println(s"estimated size: ${estimatedSizeInGB()} GB")

    (0 until numSrcNodes).toIterator.flatMap{ idx =>
      val srcNode = getSrcNode(idx)
      val adjNodes = neighborsOfNode(idx)
      val neighborsNodesMap = psModel.getNeighborTable(adjNodes)
      Array.fill(adjNodes.length)(srcNode).zip(adjNodes)
        .flatMap { case (srcNode, dstNode) =>
          val neighborsOfDst = neighborsNodesMap.get(dstNode)
          if (idx % 100000 == 0) {
            println(s"src node = $srcNode, dst node = $dstNode")
            println(s"neighbors of src node = ${adjNodes.mkString(",")}")
            println(s"neighbors of dst node = ${neighborsOfDst.mkString(",")}")
            println(s"number of common friends: ${adjNodes.intersect(neighborsOfDst).length}")
          }
          Iterator.single((srcNode, dstNode), adjNodes.intersect(neighborsOfDst).length)
      }
    }
  }
}

object CommonFriendsPartition {

}