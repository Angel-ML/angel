/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.spark.ml.gbt

import scala.collection.mutable.ArrayBuffer

case class Node(id: Int = -1) {
  var isLeaf: Boolean = false
  var leafValue: Double = 0.0
  var isActive: Boolean = false

  def this(id: Int, isActive: Boolean, isLeaf: Boolean, leafValue: Double) {
    this(id)
    this.isActive = isActive
    this.isLeaf = isLeaf
    this.leafValue = leafValue
  }
}


class Tree extends Serializable {
  var id: Int = _
  private var nodes: Array[Node] = _

  def this(id: Int, maxDepth: Int) {
    this()
    this.id = id
    val maxNodeNum = math.pow(2, maxDepth).toInt
    this.nodes = (0 until maxNodeNum).toArray.map {nid => Node()}
    this.nodes(0) = Node(0)
    this.nodes(0).isActive = true
  }

  def depth: Int = {
    var maxIndex = 0
    for (i <- Range(nodes.length - 1, -1, -1)) {
      if (this.nodes(i).id >= 0) {
        maxIndex = this.nodes(i).id
        return (math.log(maxIndex + 1) / math.log(2)).toInt + 1
      }
    }
    1
  }

  def getActiveNode: Array[Node] = {
    val activeNodes = new ArrayBuffer[Node]()
    forActive { node =>
      activeNodes.append(node)
    }
    activeNodes.toArray
  }

  def forActive(f: Node => Any): Unit = {
    var nid = 0
    while (nid  < nodes.length) {
      if (nodes(nid).isActive) {
        f(nodes(nid))
      }
     nid += 1
    }
  }

  def foreachNode(f: Node => Any): Unit = {
    var nid = 0
    while (nid < nodes.length) {
      f(nodes(nid))
      nid += 1
    }
  }

  def hasActiveNode: Boolean = {
    var hasActive = false
    nodes.foreach { node =>
      if (node.isActive) {
        hasActive = true
        return hasActive
      }
    }
    hasActive
  }

  def addChildren(nodeId: Int): Unit = {
    this.nodes(2 * nodeId + 1) = Node(2 * nodeId + 1)
    this.nodes(2 * nodeId + 2) = Node(2 * nodeId + 2)
  }

  def setLeaf(nodeId: Int, weight: Double): Unit = {
    this.nodes(nodeId).leafValue = weight
    this.nodes(nodeId).isLeaf = true
    this.nodes(nodeId).isActive = false
  }

  def setNodeActive(nodeId: Int): Unit = {
    this.nodes(nodeId).isActive = true
  }

  def setNodeInactive(nodeId: Int): Unit = {
    this.nodes(nodeId).isActive = false
  }

  override def toString: String = {
    s"tree id: $id depth: $depth active node: ${getActiveNode.map(_.id).mkString(" ")}\n" +
    s"node: ${nodes.filter(n => n.id != -1).map(_.id).mkString(" ")}"
  }
}
