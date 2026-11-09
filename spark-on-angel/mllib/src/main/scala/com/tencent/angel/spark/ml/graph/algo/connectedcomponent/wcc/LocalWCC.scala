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
import edu.princeton.cs.algs4.UF


object LocalWCC {
  def process(edge: Array[(Long, Long)]): Map[Long, Long] = {
    val nodes = edge.flatMap { case (src, dst) => Iterator(src, dst) }.distinct
    val global2local = new OpenHashMap[Long, Int](nodes.length)
    nodes.indices.foreach { i => global2local(nodes(i)) = i }

    //Union-Find
    val uf = new UF(nodes.length)
    edge.foreach { case (src, dst) =>
      uf.union(global2local(src), global2local(dst))
    }

    // local2global
    val maps: Map[Long, Long] = global2local.map { case (global, local) =>
      global -> nodes(uf.find(local))
    }(collection.breakOut)

    println(s"[wcc-local]localCC finished")
    maps
  }
}
