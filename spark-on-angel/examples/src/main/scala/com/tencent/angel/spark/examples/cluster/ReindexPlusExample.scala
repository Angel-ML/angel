package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.graph.reindex.ReIndex
import com.tencent.angel.graph.utils.GraphIO
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

object ReindexPlusExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val maps = params.getOrElse("maps", null)
    val mapsInputPath = params.getOrElse("mapsInputPath", null)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val dataPartitionNum = params.getOrElse("partitionNum", "100").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "100").toInt
    val batchSize = params.getOrElse("batchSize", "5000").toInt
    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val sc = start(mode)
    val edges: DataFrame = GraphIO.loadStringEdge(input, isWeighted, srcIndex, dstIndex, weightIndex, sep)
    val reindex = new ReIndex()
      .setPSPartitionNum(psPartitionNum)
      .setPartitionNum(dataPartitionNum)
      .setBatchSize(batchSize)
      .setIsWeighted(isWeighted)
      .setReIndexedNodesMapPath(mapsInputPath)
    reindex.transform(edges)
    reindex.save(output, maps, sep)
    stop()
  }


  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    // Close the automatic checkpoint
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_BACKUP_AUTO_ENABLE, "false")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_JVM_USE_PARALLEL_GC, "true")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_JVM_PARALLEL_GC_USE_ADAPTIVE_SIZE, "false")
    conf.set("io.file.buffer.size", "16000000");
    conf.set("spark.hadoop.io.file.buffer.size", "16000000");

    conf.setMaster(mode)
    conf.setAppName("ReIndexPlus")
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
