package com.tencent.angel.spark.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.{OneHotVector, SparseVector, Vector}
import com.tencent.angel.spark.ml.classification.SparseLRWithFTRL
import com.tencent.angel.spark.ml.util.DataLoader

object SparseLRWithFTRLTest{

  def main(args: Array[String]): Unit = {

    val input = "./spark-on-angel/mllib/src/test/data/instances"
    val alpha = 0.1
    val beta = 1.0
    val lambda1 = 0.1
    val lambda2 = 0.1
    val pDim = -1
    val partitionNum = 1
    val epoch = 1

    val output = null
    val sampleRate = 1.0
    val batchSize = 2
    val isIncrementLearn = false
    val modelPath = "./spark-on-angel/mllib/src/test/data/model_path"
    val numSampleBatch = 40
    val numPartitionBatch = 4
    val validateFaction = 0.3
    val logPath = "./spark-on-angel/mllib/src/test/data/log_path"

    runSpark(this.getClass.getSimpleName){ sc =>
      PSContext.getOrCreate(sc)

      train(input,
        sampleRate,
        partitionNum: Int,
        pDim: Long,
        epoch: Int,
        batchSize: Int,
        lambda1: Double,
        lambda2: Double,
        alpha: Double,
        beta: Double,
        modelPath: String,
        validateFaction: Double)
    }
  }


  def train(input: String,
            sampleRate: Double,
            partitionNum: Int,
            pDim: Long,
            epoch: Int,
            batchSize: Int,
            lambda1: Double,
            lambda2: Double,
            alpha: Double,
            beta: Double,
            modelPath: String,
            validateFaction: Double) = {
    val isOneHot = DataLoader.isOneHotType(input, sampleRate, partitionNum)

    val (instances, dim) = if(!isOneHot) {

      // SparseVector
      val tempInstances = DataLoader.loadSparseInstance(input, partitionNum, sampleRate)
        .map{case (feat, label) =>
          val featAddInt =  Array((0L, 1.0)) ++ feat
          (featAddInt, label)
        }

      val dim = if (pDim > 0) {
        pDim
      } else {
        tempInstances.flatMap { case (feat, label) =>
          feat.map(_._1).distinct
        }.distinct().count()
      }
      println(s"feat number: $dim")

      val svInstances = tempInstances.map { case (feat, label) =>
        val featV: Vector = new SparseVector(dim.toLong, feat)
        (featV, label)
      }.repartition(partitionNum)

      (svInstances, dim)
    } else {
      // OneHotVector
      val tempInstances = DataLoader.loadOneHotInstance(input, partitionNum, sampleRate).rdd
        .map { row =>
          Tuple2(row.getAs[scala.collection.mutable.WrappedArray[Long]](1).toArray, row.getString(0).toDouble)
        }.map { case (feat, label) =>
        (0L +: feat, label)
      }

      val dim = if (pDim > 0) {
        pDim
      } else {
        tempInstances.flatMap { case (feat, label) =>
          feat.distinct
        }.distinct().count()
      }

      println(s"feat number: $dim")

      val ovInstances = tempInstances.map { case (feat, label) =>
        val featV: Vector = new OneHotVector(dim.toLong, feat)
        (featV, label)
      }

      (ovInstances, dim)
    }

    instances.cache()
    val sampleNum = instances.count()
    val posSamples = instances.filter(_._2 == 1.0).count()
    val negSamples = instances.filter(_._2 == 0.0).count()
    println(s"total count: $sampleNum posSample: $posSamples negSamples: $negSamples")
    require(posSamples + negSamples == sampleNum, "labels must be 0 or 1")

    val model = SparseLRWithFTRL.train(instances, sampleNum, dim, epoch, batchSize, lambda1, lambda2, alpha, beta, validateFaction)
    model.save(modelPath)

    instances.unpersist()
  }

  def runSpark(name: String)(body: SparkContext => Unit): Unit = {
    println("this is in run spark")
    val conf = new SparkConf
    val master = conf.getOption("spark.master")
    val isLocalTest = if (master.isEmpty || master.get.toLowerCase.startsWith("local")) true else false
    val sparkBuilder = SparkSession.builder().appName(name)
    if (isLocalTest) {
      sparkBuilder.master("local")
        .config("spark.ps.mode", "LOCAL")
        .config("spark.ps.jars", "")
        .config("spark.ps.instances", "1")
        .config("spark.ps.cores", "1")
    }
    val sc = sparkBuilder.getOrCreate().sparkContext
    body(sc)
    val wait = sys.props.get("spark.local.wait").exists(_.toBoolean)
    if (isLocalTest && wait) {
      println("press Enter to exit!")
      Console.in.read()
    }
    sc.stop()
  }

}