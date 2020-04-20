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
package com.tencent.angel.spark.ml.graph.connectedcomponent.scc

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.tencent.angel.spark.ml.graph.utils.collection.OpenHashMap
import scala.collection.mutable.ArrayBuffer

object LocalSCC {


  def process(edge: RDD[(Long, Long)]):Seq[(Long, Long)] = {

    val nodes = edge.flatMap { case (src, dst) =>
      Iterator(src, dst)
    }.distinct.collect()

    println(s"[localSCC]numNodes=${nodes.length}")

    val cc = tarjan(edge: RDD[(Long, Long)], nodes)
    val min = Array.fill[Long](cc.length)(Long.MaxValue)
    nodes.indices.foreach { i =>
      val id = cc(i) - 1
      min(id) = math.min(min(id), nodes(i))
    }
    val tarjanRet = nodes.indices.map { i =>
      (nodes(i), min(cc(i) - 1))
    }

    println("localSCC finished")
    tarjanRet
  }

  def tarjan(edge: RDD[(Long, Long)], nodes: Array[Long]): Array[Int] = {
    val maps = new OpenHashMap[Long, Int](nodes.length)
    for (i <- nodes.indices) maps(nodes(i)) = i
    val bcNodes = edge.context.broadcast(maps)
    val localEdges = edge.mapPartitions { iter =>
      val maps = bcNodes.value
      iter.map { case (src, dst) =>
        (maps(src), maps(dst))
      }
    }.distinct().collect()
    val adj = Array.fill(nodes.length)(new ArrayBuffer[Int](0))
    localEdges.foreach { case (src, dst) =>
      adj(src) += dst
    }
    val begin = System.currentTimeMillis()
    val n = adj.length
    println(s"[Tarjan]n = $n")
    val dfn = new Array[Int](n)
    val low = new Array[Int](n)
    val flag = Array.fill(n)(-1) // -1 -- not visited, 0 -- in tarjanStack,  x(> 0) -- popped, belongs to component x
    val tarjanStack = new Array[Int](n)
    var tarjanStackPos = -1
    val nonRecursiveStack = new Array[Int](n) // for nonRecursive Implements
    var nonRecursiveStackPos = -1
    var dfsIndex = 0
    var wccIndex = 0
    val cur = Array.fill(n)(0)

    for (v <- 0 until n) {
      if (flag(v) < 0) {
        nonRecursiveStackPos += 1
        nonRecursiveStack(nonRecursiveStackPos) = v
        dfsIndex += 1
        dfn(v) = dfsIndex
        low(v) = dfsIndex
        tarjanStackPos += 1
        tarjanStack(tarjanStackPos) = v
        flag(v) = 0
        while (nonRecursiveStackPos >= 0) {
          val t = nonRecursiveStack(nonRecursiveStackPos)
          var notBreak = true
          while (cur(t) < adj(t).length && notBreak) {
            val u = adj(t)(cur(t))
            cur(t) += 1
            if (flag(u) < 0) {
              dfsIndex += 1
              dfn(u) = dfsIndex
              low(u) = dfsIndex
              tarjanStackPos += 1
              tarjanStack(tarjanStackPos) = u
              nonRecursiveStackPos += 1
              nonRecursiveStack(nonRecursiveStackPos) = u
              flag(u) = 0
              notBreak = false
            }
          }
          if (t == nonRecursiveStack(nonRecursiveStackPos)) {
            for (u <- adj(t)) {
              if (dfn(u) > dfn(t)) low(t) = math.min(low(t), low(u))
              else if (flag(u) == 0) low(t) = math.min(low(t), dfn(u))
            }
            if (dfn(t) == low(t)) {
              wccIndex += 1
              var j = -1
              do {
                j = tarjanStack(tarjanStackPos)
                tarjanStackPos -= 1
                flag(j) = wccIndex
              } while (j != t)
            }
            nonRecursiveStackPos -= 1
          }
        }
      }
    }
    println(s"[Tarjan]cost = ${(System.currentTimeMillis() - begin) / 1000.0}s")
    flag
  }

  def main(args: Array[String]): Unit = {

    val err = Array[(Long,Long)]((1L,2L),(2L,5L),(5L,1L),(5L,6L),(6L,7L),(5L,8L),(7L,8L),(7L,9L),(9L,10L),(10L,7L))
    val vrr = Array[Long](1L,2L,3L,5L,6L,7L,8L,9L,10L)
    val sc = new SparkContext("local"," localSCC")
    val edd = sc.parallelize(err)
    val res = process(edd)
    println(res)
  }
}
