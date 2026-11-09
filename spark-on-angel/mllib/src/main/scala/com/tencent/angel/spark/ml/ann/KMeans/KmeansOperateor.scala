package com.tencent.angel.spark.ml.ann.KMeans

import java.util.concurrent.{Callable, Executors, FutureTask}

import com.tencent.angel.spark.ml.util.DFStruct
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.ml.linalg
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.collection.mutable.ArrayBuffer
import scala.math.ceil

object KmeansOperateor {

  def threadRun(clusterIDForDistribute: Array[Array[Int]], data: DataFrame, ClusterNumber2: Array[Int], cc: Array[Int],
                maxIterationLevel2: Int, epsilonLevel2: Double, initModeLevel2: String, sep: String, outputLevel2: String, outputCenter: String, partitionNumberForThread: Int, fs: FileSystem, kmeansAlgo: String = "KMeans"): Unit = {

    val numsThreads = clusterIDForDistribute.size
    println("the threads number is " + numsThreads)

    //启动多个线程
    val jobExecutor = Executors.newFixedThreadPool(numsThreads)
    val futureTasklist = new ArrayBuffer[FutureTask[Long]]()

    for (i <- 0 until numsThreads) {
      val future = new FutureTask[Long](new Callable[Long] {
        override def call(): (Long) = {
          val count = KMForOneList(clusterIDForDistribute(i), data, ClusterNumber2, cc, maxIterationLevel2, epsilonLevel2, initModeLevel2, sep, outputLevel2 + i, outputCenter + i, partitionNumberForThread, fs, kmeansAlgo)
          count
        }
      })
      futureTasklist.append(future)
    }

    try {
      futureTasklist.foreach(task => jobExecutor.execute(task))
      var dataNumber = 0L
      futureTasklist.foreach { future =>
        val number = future.get()
        dataNumber += number
        println("the number of data for level list " + number)
      }
      println("the number of all data in one threadRun " + dataNumber)

    }
    finally {
      jobExecutor.shutdown() //关闭线程
    }
  }


  def KMForOneList(levelArray: Array[Int], data: DataFrame, ClusterNumber2: Array[Int], cc: Array[Int],
                   maxIterationLevel2: Int, epsilonLevel2: Double, initModeLevel2: String, sep: String, outputLevel2: String, outputCenter: String, partitionNumberForThread: Int, fs: FileSystem, kmeansAlgo: String): Long = {

    val sc = SparkContext.getOrCreate()

    val outputLevel2Path = new Path(outputLevel2)
    if (fs.exists(outputLevel2Path)) {
      fs.delete(outputLevel2Path, true)
    }

    val outputCenterPath = new Path(outputCenter)
    if (fs.exists(outputCenterPath)) {
      fs.delete(outputCenterPath, true)
    }


    val dataForlevelList = data.where(col("predictionLevel1").isin(levelArray: _*)).repartition(partitionNumberForThread).cache()

    var dataNumberOneThread = 0L
    levelArray.foreach { clusterIDLevel1 =>
      val start1 = System.currentTimeMillis
      val dataTmp = dataForlevelList.filter(s"predictionLevel1 == $clusterIDLevel1").cache()
      val ClusterNumber2Tmp = ClusterNumber2(clusterIDLevel1)
      val prefix = cc(clusterIDLevel1)
      val dataTmpNumber = dataTmp.count()
      dataNumberOneThread += dataTmpNumber
      val ClusterNumber2Final = if (dataTmpNumber > ClusterNumber2Tmp) ClusterNumber2Tmp else dataTmpNumber.toInt
      println(clusterIDLevel1, ClusterNumber2Tmp, prefix)

      val (resultTmp, centers) = if (kmeansAlgo == "KMeans") {
        val kmTmp = new KMeans()
          .setK(ClusterNumber2Final)
          .setMaxIter(maxIterationLevel2)
          .setTol(epsilonLevel2)
          .setFeaturesCol(DFStruct.FEATURE)
          .setInitMode(initModeLevel2)

        val KMModel = kmTmp.fit(dataTmp)
        val resultTmp = KMModel.transform(dataTmp).cache()
        val centers = KMModel.clusterCenters.map(a => a.toArray)
        (resultTmp, centers)

      }
      else {
        val kmTmp = new BisectingKMeans()
          .setK(ClusterNumber2Final)
          .setMaxIter(maxIterationLevel2)
          .setFeaturesCol(DFStruct.FEATURE)

        val KMModel = kmTmp.fit(dataTmp)
        val resultTmp = KMModel.transform(dataTmp).cache()
        val centers = KMModel.clusterCenters.map(a => a.toArray)
        (resultTmp, centers)

      }

      val vecToArray = udf((xs: linalg.Vector) => xs.toArray.mkString(" "))
      val resultTmpToSave = resultTmp.withColumn("feature", vecToArray(col("feature"))).drop("predictionLevel1").withColumn("prediction", col("prediction") + prefix)

      resultTmpToSave.write.option("header", false).mode(SaveMode.Append).option("delimiter", ":").csv(outputLevel2)

      val centerRDD = sc.parallelize(Array.range(0, centers.length).map { i => Row(i + prefix + ":" + centers(i).mkString(" ")) })
      val schema = StructType(Seq(StructField("center", StringType, nullable = true)))
      val orderDF = data.sparkSession.createDataFrame(centerRDD, schema)
      orderDF.write.option("header", false).mode(SaveMode.Append).option("delimiter", sep).option("quote", "").csv(outputCenter)

      resultTmp.unpersist()
      dataTmp.unpersist()
      val end1 = System.currentTimeMillis

      println("the dataNumber for clusterIDLevel1 " + clusterIDLevel1 + " is " + dataTmpNumber)
      println("the cost time for clusterIDLevel1 " + clusterIDLevel1 + " is " + {
        (end1 - start1) / 1000.0
      } + " seconds")

    }
    dataNumberOneThread
  }


  def splitIDsToMultiThreads(listTest: Array[Int], threadNumber: Int): Array[Array[Int]] = {
    val colNumber = ceil(listTest.length / threadNumber.toDouble).toInt //col number
    println(listTest.length, threadNumber, colNumber)
    val resutl = Array.fill(threadNumber, colNumber)(-1) // init all elements//Array.fill(threadNumber, colNumber)(-1)

    var dataIndex = 0
    Array.range(0, colNumber).foreach { col =>
      if (col % 2 == 0) {
        Array.range(0, threadNumber).foreach { row =>
          if (dataIndex < listTest.length) {
            resutl(row)(col) = listTest(dataIndex)
            dataIndex += 1
          }
        }
      }
      else {
        Array.range(threadNumber - 1, -1, -1).foreach { row =>
          if (dataIndex < listTest.length) {
            resutl(row)(col) = listTest(dataIndex)
            dataIndex += 1
          }
        }
      }
    }

    val threadResult = resutl.zipWithIndex.map { item => if (item._2 % 2 == 1) item._1.reverse.filter(_ != -1) else item._1.filter(_ != -1) }
    threadResult
  }
}
