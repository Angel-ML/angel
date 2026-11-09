package com.tencent.angel.spark.ml.ann.KMeans

import com.tencent.angel.spark.ml.ann.KMeans.KmeansOperateor.splitIDsToMultiThreads
import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KMeansIVFHnswLocal {
  def main(args: Array[String]): Unit = {

    val input = "data/mlTestData/IVFHnsw_data_test.txt" //xxxx.txt,poker_hand.csv
    val featureCols = "1-4" //"1-20"
    val sep = ","
    val mode = "local"
    val sampleRate = 1
    val partitionNum = 1
    val idCol = 0

    val clusterNumber = 10
    val ClusterNumberLevel1 = 4 //sqrt(clusterNumber).toInt
    val cc = Array.range(0, ClusterNumberLevel1 + 1).map{ x =>x * (clusterNumber / ClusterNumberLevel1)}
    val ClusterNumber2 = Array.range(0, cc.length - 1).map { x => cc(x + 1) - cc(x) }


    val DistributeThreshold = 25

    val outputLevel2 = "data/output/outputLevel2/"
    val outputCenter = "data/output/outputCenter/"

    val initModeLevel1 = "k-means||" // "random" or "k-means||"
    val maxIterationLevel1 = 10
    val epsilonLevel1 = 1e-4

    val initModeLevel2 = "k-means||" // "random" or "k-means||"
    val maxIterationLevel2 = 10
    val epsilonLevel2 = 0.001

    val threadNumberForSmall = 2
    val threadNumberForLarge = 2
    val partitionNumberForThreadForLarge = 1
    val partitionNumberForThreadForSmall = 1

    val kmeansAlgo = "KMeans"//KMeans

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


    KmeansOperateor.threadRun(splitResultForLarge, dataLargeCluster, ClusterNumber2, cc, maxIterationLevel2, epsilonLevel2, initModeLevel2, sep, outputLevel2 + "l", outputCenter + "l", partitionNumberForThreadForLarge, fs: FileSystem,kmeansAlgo)
    KmeansOperateor.threadRun(splitResultForSmall, dataSmallCluster, ClusterNumber2, cc, maxIterationLevel2, epsilonLevel2, initModeLevel2, sep, outputLevel2 + "s", outputCenter + "s", partitionNumberForThreadForSmall, fs: FileSystem,kmeansAlgo)
    val end2 = System.currentTimeMillis
    println(s"KMeans level 2 cost : " + {
      (end2 - start2) / 1000.0
    } + " seconds")

    //dataSmallCluster.show(5)
    //println(dataSmallCluster.schema)
    //println(dataLargeCluster.count(), dataSmallCluster.count())


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
