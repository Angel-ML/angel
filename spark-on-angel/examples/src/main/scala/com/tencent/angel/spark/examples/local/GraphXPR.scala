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
package com.tencent.angel.spark.examples.local

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.graphx.lib.PageRank2
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphXPR {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = "data/bc/toy"
    val output = "model/graphx/toy"
    val tol = params.getOrElse("tol", "0.01").toDouble
    val resetProp = params.getOrElse("resetProp", "0.15").toDouble


    start("local")
    var edges: RDD[Edge[Double]] = null
    val df = GraphIO.load(input,
      isWeighted = false)

    edges = df.select("src", "dst").rdd.flatMap {
      case row =>
        val src = row.getLong(0)
        val dst = row.getLong(1)
        if (src != dst)
          Iterator.single(Edge(src, dst, 1.0))
        else
          Iterator.empty
    }.repartition(1)


    val graph = Graph.fromEdges(edges, 1)
//    val pr = graph.pageRank(tol, resetProp)
    val pr = PageRank2.runUntilConvergence(graph, tol, resetProp)
    val ranks = pr.vertices.map(f => s"${f._1} ${f._2}")
    ranks.saveAsTextFile(output)
  }


  def start(mode: String): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("GraphXPageRank")
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("INFO")
    //    sc.setCheckpointDir("cp")
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }

}
