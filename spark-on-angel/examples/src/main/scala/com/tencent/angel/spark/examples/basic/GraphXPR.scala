package com.tencent.angel.spark.examples.basic

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.graphx.lib.PageRank3
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

object GraphXPR {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", "")
    val output = params.getOrElse("output", "")
    val tol = params.getOrElse("tol", "0.01").toFloat
    val resetProp = params.getOrElse("resetProp", "0.15").toFloat
    val mode = params.getOrElse("mode", "yarn-cluster")
    val isWeight = params.getOrElse("isWeight", "false").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val partitionNum = params.getOrElse("partitionNum", "10").toInt

    start(mode)
    var edges: RDD[Edge[Float]] = null
    val df = GraphIO.load(input,
      isWeighted = isWeight,
      srcIndex = srcIndex,
      dstIndex = dstIndex,
      weightIndex = weightIndex)

    if (isWeight) {
      edges = df.select("src", "dst", "weight").rdd.flatMap {
        case row =>
          val src = row.getLong(0)
          val dst = row.getLong(1)
          val weight = row.getFloat(2)
          if (src != dst)
            Iterator.single(Edge(src, dst, weight))
          else
            Iterator.empty
      }.repartition(partitionNum)
    } else {
      edges = df.select("src", "dst").rdd.flatMap {
        case row =>
          val src = row.getLong(0)
          val dst = row.getLong(1)
          if (src != dst)
            Iterator.single(Edge(src, dst, 1.0f))
          else
            Iterator.empty
      }.repartition(partitionNum)
    }



    val graph = Graph.fromEdges(edges, 1)
//    graph.inDegrees.map(f => s"${f._1} ${f._2}").saveAsTextFile(output)

    println(s"numEdges=${graph.edges.count()} numNodes=${graph.vertices.count()}")
//    val pr = graph.pageRank(0.01, 0.15)
//    val pr = PageRank2.runUntilConvergence(graph, 0.001, 0.15)
//    val ranks = pr.vertices.map(f => s"${f._1} ${f._2}")
//    ranks.saveAsTextFile(output)
    val pr = PageRank3.runDynamic(graph, tol, resetProp)
    val ranks = pr.map(f => s"${f(0)} ${f(1)}")
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
