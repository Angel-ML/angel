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
package com.tencent.angel.spark.ml.graph.algo.connectedcomponent.wcc

import com.tencent.angel.spark.ml.graph.utils.collection.OpenHashMap
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList
import edu.princeton.cs.algs4.UF

class CutCCGraphPartition(index: Int,
                          keys: Array[Long],
                          indptr: Array[Int],
                          neighbors: Array[Long],
                          keyLabels: Array[Long],
                          nodeLabels: Array[Long],
                          nodes: Array[Long]) extends Serializable {
  
  // if label of node is larger than its neighbors',
  // change it into min among its neighbors' labels
  def process(): Unit = {
    val global2local = new OpenHashMap[Long, Int](nodes.length)
    nodes.indices.foreach{ i => global2local(nodes(i)) = i }
    
    // union-find
    val uf = new UF(nodes.length)
    for (idx <- keys.indices) {
      var j = indptr(idx)
      while (j < indptr(idx + 1)) {
        uf.union(global2local(keys(idx)), global2local(neighbors(j)))
        j += 1
      }
    }
    
    // local2global
    //    val maps: Map[Long, Long] = global2local.map { case (global, local) =>
    //      global -> nodes(uf.find(local))
    //    }(collection.breakOut)
    //
    keys.indices.foreach{ idx =>
      keyLabels(idx) = nodes(uf.find(global2local(keys(idx))))
    }
    
    nodes.indices.foreach{ idx =>
      nodeLabels(idx) = nodes(uf.find(global2local(nodes(idx))))
    }
  }
  
  def savePart(): (Array[Long], Array[Long]) = {
    (nodes, nodeLabels)
  }
  
  def save(): (Array[Long], Array[Long]) = {
    (keys, keyLabels)
  }
  
}

object CutCCGraphPartition {
  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])]): CutCCGraphPartition = {
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbors = new LongArrayList()
    
    indptr.add(0)
    
    while (iterator.hasNext) {
      val (node, ns) = iterator.next()
      keys.add(node)
      ns.toArray.distinct.foreach(n => neighbors.add(n))
      indptr.add(neighbors.size())
    }
    
    val keysArray = keys.toLongArray()
    val neighborsArray = neighbors.toLongArray()
    val indicesArray = keysArray.union(neighborsArray).distinct
    new CutCCGraphPartition(index, keysArray, indptr.toIntArray(),
      neighborsArray, new Array[Long](keysArray.length),new Array[Long](indicesArray.length), indicesArray)
  }
}
