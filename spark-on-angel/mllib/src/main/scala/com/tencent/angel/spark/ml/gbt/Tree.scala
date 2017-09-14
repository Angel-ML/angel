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

case class Node(id: Int) {
  private var isLeaf: Boolean = false
  private var leafValue: Double = _

  var isActive: Boolean = false


}


class Tree(maxNodeNum: Int) {

  private val nodes = new Array[Node](maxNodeNum)

  def getActiveNode: Array[Node] = {
    val activeNodes = new ArrayBuffer[Node]()
    forActive { node =>
      activeNodes.append(node)
    }
    activeNodes.toArray
  }

  def forActive(f: Node => Any): Unit = {
    var nid = 0
    while (nid  < maxNodeNum) {
      if (nodes(nid).isActive) {
        f(nodes(nid))
      }
     nid += 1
    }
  }
}
