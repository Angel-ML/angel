package com.tencent.angel.spark.examples.cluster.ann

import com.tencent.angel.spark.ml.ann.KMeans.KmeansOperateor
import com.tencent.angel.spark.ml.ann.KMeans.KmeansOperateor.splitIDsToMultiThreads
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object KMeansIVFHnswCluster {
  def main(args: Array[String]): Unit = {

    val params = coreArgsUtil.parse(args)

    val mode = params.getOrElse("mode", "yarn")
    val input = params.getOrElse("input", null)
    val outputCenter = params.getOrElse("outputCenter", null)
    val outputLevel2 = params.getOrElse("outputLevel2", null)
    val featureCols = params.getOrElse("featureCols", null)
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val partitionNum = params.getOrElse("partitionNum", "4").toInt
    val idCol = params.getOrElse("idCol", "0").toInt

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val clusterNumber = params.getOrElse("clusterNumber", "1000").toInt
    val ClusterNumberLevel1 = params.getOrElse("ClusterNumberLevel1", "20").toInt
    val DistributeThreshold = params.getOrElse("DistributeThreshold", "20").toInt

    val initModeLevel2 = params.getOrElse("initModeLevel2", MLlibKMeans.K_MEANS_PARALLEL) // "random" or "k-means||"
    val maxIterationLevel2 = params.getOrElse("maxIterationLevel2", "10").toInt
    val epsilonLevel2 = params.getOrElse("epsilonLevel2", "1e-4").toDouble

    val initModeLevel1 = params.getOrElse("initModeLevel1", MLlibKMeans.K_MEANS_PARALLEL) // "random" or "k-means||"
    val maxIterationLevel1 = params.getOrElse("maxIterationLevel1", "10").toInt
    val epsilonLevel1 = params.getOrElse("epsilonLevel1", "1e-4").toDouble

    val threadNumberForSmall = params.getOrElse("threadNumberForSmall", "20").toInt
    val threadNumberForLarge = params.getOrElse("threadNumberForLarge", "20").toInt
    val partitionNumberForThreadForLarge = params.getOrElse("partitionNumberForThreadForLarge", "20").toInt
    val partitionNumberForThreadForSmall = params.getOrElse("partitionNumberForThreadForSmall", "20").toInt

    val kmeansAlgo = params.getOrElse("kmeansAlgo", "KMeans")


    val cc = Array.range(0, ClusterNumberLevel1 + 1).map { x => x * (clusterNumber / ClusterNumberLevel1) }
    val ClusterNumber2 = Array.range(0, cc.length - 1).map { x => cc(x + 1) - cc(x) }
    println("ClusterNumberLevel1 " + ClusterNumberLevel1)
    println(cc.mkString(","))
    println(ClusterNumber2.mkString(","))


    val sc = start(mode)

    val conf = sc.hadoopConfiguration
    val outputLevel2Path = new Path(outputLevel2)
    val fs = outputLevel2Path.getFileSystem(conf)

    val data = DataLoader.loadlabeled(sc, input, partitionNum, sampleRate, featureCols, idCol, false, sep).persist(StorageLevel.DISK_ONLY)

    val start1 = System.currentTimeMillis

    val KMLevel1 = new KMeans()
      .setK(ClusterNumberLevel1)
      .setMaxIter(maxIterationLevel1)
      .setTol(epsilonLevel1)
      .setFeaturesCol(DFStruct.FEATURE)
      .setInitMode(initModeLevel1)

    val KMModelLevel1 = KMLevel1.fit(data)
    val resultClusterLevel1 = KMModelLevel1.transform(data).withColumnRenamed("prediction", "predictionLevel1").persist(StorageLevel.DISK_ONLY)
    val end1 = System.currentTimeMillis
    println(s"KMeans level 1 cost : " + {
      (end1 - start1) / 1000.0
    } + " seconds")

    val summary = resultClusterLevel1.select("predictionLevel1").rdd.map {
      case Row(x: Int) => (x, 1)
    }.reduceByKey(_ + _).collect().sortBy(_._2).reverse

    println("summary " + summary.mkString(","))

    val clusterIDForLarge = summary.filter(_._2 > DistributeThreshold).map(_._1)
    val clusterIDForSmall = summary.filter(_._2 <= DistributeThreshold).map(_._1)

    println("clusterIDForDistribute")
    println(clusterIDForLarge.mkString(","))

    val start2 = System.currentTimeMillis
    val dataLargeCluster = resultClusterLevel1.where(col("predictionLevel1").isin(clusterIDForLarge: _*))
    val dataSmallCluster = resultClusterLevel1.where(!col("predictionLevel1").isin(clusterIDForLarge: _*))

    val splitResultForLarge = splitIDsToMultiThreads(clusterIDForLarge, threadNumberForLarge)
    val splitResultForSmall = splitIDsToMultiThreads(clusterIDForSmall, threadNumberForSmall)

    KmeansOperateor.threadRun(splitResultForLarge, dataLargeCluster, ClusterNumber2, cc, maxIterationLevel2, epsilonLevel2, initModeLevel2, sep, outputLevel2 + "l", outputCenter + "l", partitionNumberForThreadForLarge, fs, kmeansAlgo)
    KmeansOperateor.threadRun(splitResultForSmall, dataSmallCluster, ClusterNumber2, cc, maxIterationLevel2, epsilonLevel2, initModeLevel2, sep, outputLevel2 + "s", outputCenter + "s", partitionNumberForThreadForSmall, fs, kmeansAlgo)

    val end2 = System.currentTimeMillis
    println(s"KMeans level 2 cost : " + {
      (end2 - start2) / 1000.0
    } + " seconds")

    stop()
  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }

}
