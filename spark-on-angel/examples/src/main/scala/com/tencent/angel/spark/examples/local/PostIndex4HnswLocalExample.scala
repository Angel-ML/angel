package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.postindex.PostIndex4Hnsw
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.{SparkConf, SparkContext}

object PostIndex4HnswLocalExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", "data/bc/postindex4hnswEdge")
    val output = params.getOrElse("output", "model/postindex4hnsw")
    val mapsInputPath = params.getOrElse("mapsInputPath", "data/bc/postindex4hnswMap")
    val isOldFirst = params.getOrElse("isOldFirst", "false").toBoolean
    val mode = params.getOrElse("mode", "local")
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val dataPartitionNum = params.getOrElse("partitionNum", "4").toInt
    val storageLevel = params.getOrElse("storageLevel", "MEMORY_ONLY")

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
      case "bar" => "\\|"
    }
    val mapSep = params.getOrElse("mapSep", "colon") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
      case "bar" => "\\|"
    }

    val sc = start(mode)

    val inputRDD = GraphIO.loadBiGraph(input, false, srcIndex, dstIndex, sep = sep)
    val mapping = GraphIO.loadStringEdge(mapsInputPath, false, srcIndex, dstIndex, sep = mapSep)

    val index = new PostIndex4Hnsw()
      .setPartitionNum(dataPartitionNum)
      .setStorageLevel(storageLevel)
      .setIsOldFirst(isOldFirst)
    val result = index.transform(inputRDD, mapping)
    GraphIO.save(result, output, sep)
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

    // Add jvm parameters for executors
    val defaultExecutorJvmOptions = "-Djava.library.path=$JAVA_LIBRARY_PATH" +
      " -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<LOG_DIR>/gc.log " +
      "-XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
      "-XX:InitiatingHeapOccupancyPercent=50 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 "

    val executorJvmOptions = conf.get("spark.executor.extraJavaOptions", defaultExecutorJvmOptions)

    println(s"executorJvmOptions = $executorJvmOptions")
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)

    conf.setMaster(mode)
    conf.setAppName("PostIndex")
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }
}
