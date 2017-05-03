package com.tencent.angel.spark.examples.util

import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PSExamples {
  var N = 2000  // Number of data points
  var DIM = 1000  // Number of dimensions
  var ITERATIONS = 5
  var numSlices = 2

  def parseArgs(args: Array[String]): Unit = {
    if (args.length == 3) {
      N = args(0).toInt
      DIM = args(1).toInt
      numSlices = args(2).toInt
    }
  }

  def runWithSparkContext(name: String)(body: SparkContext => Unit): Unit = {
    val isLocalTest = (new SparkConf).getOption("spark.master").isEmpty
    val useLocalCluster = sys.props.get("spark.local.cluster").exists(_.toBoolean)
    val sparkBuilder = SparkSession.builder().appName(name)
    if (isLocalTest) {
      var tmpDir = "/tmp"
      if (sys.props("os.name").startsWith("Windows")) {
        tmpDir = sys.env.getOrElse("SPARK_TMP", "D:\\tmp")
      }
      sparkBuilder.config("spark.local.dir", tmpDir)
      if (useLocalCluster) {
        sys.props("spark.testing") = "1"
        sys.props("spark.test.home") = tmpDir
        val psOutDir = Paths.get(tmpDir, "ps-out").toString
        val psTmpDir = Paths.get(tmpDir, "ps-tmp").toString
        sparkBuilder.master("local-cluster[2, 1, 128]")
          .config("spark.memory.useLegacyMode", "true")
          .config("spark.executor.memory", "128m")
          .config("spark.cores.max", 2)
          .config("spark.ps.instances", 2)
          .config("spark.ps.cores", 1)
          .config("spark.ps.memory", "128m")
          .config("spark.ps.out.path", s"file:///$psOutDir")
          .config("spark.ps.out.tmp.path", s"/$psTmpDir")
          .config("spark.ps.block.cols.min", DIM / 2)
      } else {
        sparkBuilder.master("local[2]")
      }
    }
    val sc = sparkBuilder.getOrCreate().sparkContext
    body(sc)
    val wait = sys.props.get("spark.local.wait").exists(_.toBoolean)
    if (isLocalTest && wait) {
      println("press Enter to exit!")
      Console.in.read()
    }
    sc.stop()
    System.exit(0)
  }
}
