package com.tencent.angel.spark.ml.graph.algo.communitydetection

import com.tencent.angel.spark.context.PSContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.Edge

object Modularity {

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "local")
    val input = params.getOrElse("input", "E:\\angel-things\\Graph\\nmi-test\\netInput\\100.dat")
    val output = params.getOrElse("output", null)
    val community = params.getOrElse("community", "E:\\angel-things\\Graph\\nmi-test\\netInput\\100community_single.dat")
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val weightIndex = params.getOrElse("weightCol", "2").toInt
    val sep = params.getOrElse("sep", "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val sc = start(mode)
//    val vertex = GraphIO.load(community, isWeighted = false, srcIndex, dstIndex, sep = sep)
//      .select("src", "dst").rdd
//      .map(row => (row.getLong(0), row.getLong(1)))

    // use the first community label when there exists multiple ones
    val vertex = sc.textFile(community).map(_.split("\t")
      .map(_.replace("\"", "").toLong))
      .map(x => (x(0), x(1)))

    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)
    val edge_ = if (isWeighted) {
      df.select("src", "dst", "weight").rdd
        .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
        .filter(f => f._1 != f._2)
        .filter(f => f._3 != 0)
    } else {
      df.select("src", "dst").rdd
        .map(row => (row.getLong(0), row.getLong(1), 1.0f))
        .filter(f => f._1 != f._2)
    }
    edge_.count()
    val edge = edge_.map( x => Edge(x._1, x._2, x._3))

//    edge.persist(StorageLevel.DISK_ONLY)

    val (inWeights, totalWeights, squareTotalWeights) =  {
      Graph[Long, Float](vertex, edge, -1, storageLevel, storageLevel)
        .triplets.flatMap { e =>
        if (e.srcAttr == e.dstAttr)
          Iterator((e.srcAttr, (2.0f * e.attr, 2.0f * e.attr)))
        else
          Iterator((e.srcAttr, (0.0f, e.attr)), (e.dstAttr, (0.0f, e.attr)))
      }.reduceByKey { case ((in1, to1), (in2, to2)) =>
        (in1 + in2, to1 + to2)
      }.map { case (_, (inWeight, totWeighs)) =>
        (inWeight, totWeighs, Math.pow(totWeighs, 2.0))
      }.reduce { case ((in1, tot1, square1), (in2, tot2, square2)) =>
        (in1 + in2, tot1 + tot2, square1 + square2)
      }
    }
    val modularity = (inWeights - squareTotalWeights / totalWeights) / totalWeights
    println(s"modularity = $modularity")
//    if (output.nonEmpty)
//      Out.saveResult(sc.makeRDD(Seq(Array(modularity.toString)), 1), output)
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    if (!mode.equals("local")) {
      // Add jvm parameters for executors
      var executorJvmOptions = conf.get("spark.executor.extraJavaOptions")
      executorJvmOptions += " -XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
        "-XX:InitiatingHeapOccupancyPercent=50 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 -Xss4M "
      conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
      println(s"executorJvmOptions = ${executorJvmOptions}")
    }

    conf.setMaster(mode)
    conf.setAppName("copra")
    val sc = new SparkContext(conf)
    //PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }
}
