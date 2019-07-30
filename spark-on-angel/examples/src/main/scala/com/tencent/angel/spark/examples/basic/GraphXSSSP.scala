package com.tencent.angel.spark.examples.basic

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphXSSSP {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val input = params.getOrElse("input", "")
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val output = params.getOrElse("output", "")
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt
    val isWeight = params.getOrElse("isWeight", "false").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val sourceId = params.getOrElse("sourceId", "5988").toInt

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
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )

    sssp.vertices.map(f => s"${f._1} ${f._2}").saveAsTextFile(output)
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("GraphXSSSP")
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }

}
