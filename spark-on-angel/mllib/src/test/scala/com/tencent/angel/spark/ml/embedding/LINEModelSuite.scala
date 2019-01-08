package com.tencent.angel.spark.ml.embedding

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import com.tencent.angel.spark.ml.embedding.line.LINEModel
import com.tencent.angel.spark.ml.{PSFunSuite, SharedPSContext}

class LINEModelSuite extends PSFunSuite with SharedPSContext {
  val input = "../../data/bc/edge"
  val output = "model/"
  val numPartition = 4
  val lr = 0.025f
  val dim = 32
  val batchSize = 1024
  val numPSPart = 2
  val numEpoch = 2
  val negative = 5
  val storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  var param: Param = _
  var data: RDD[(Int, Int)] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = sc.textFile(input).mapPartitions { iter =>
      val r = new Random()
      iter.map { line =>
        val arr = line.split(" ")
        val src = arr(0).toInt
        val dst = arr(1).toInt
        (r.nextInt, (src, dst))
      }
    }.repartition(numPartition).values.persist(storageLevel)
    val numEdge = data.count()
    val maxNodeId = data.map { case (src, dst) => math.max(src, dst) }.max().toLong + 1
    println(s"numEdge=$numEdge maxNodeId=$maxNodeId")

    param = new Param()
    param.setLearningRate(lr)
    param.setEmbeddingDim(dim)
    param.setBatchSize(batchSize)
    param.setNumPSPart(Some(numPSPart))
    param.setNumEpoch(numEpoch)
    param.setNegSample(negative)
    param.setMaxIndex(maxNodeId)
    param.setModelCPInterval(1000)
  }

  test("first order") {
    param.setOrder(1)
    val model = new LINEModel(param) {
      val messages = new ArrayBuffer[String]()

      // mock logs
      override def logTime(msg: String): Unit = {
        if (null != messages) {
          messages.append(msg)
        }
        println(msg)
      }
    }
    model.train(data, param, "")
    model.save(output, 0)
    model.destroy()

    // extract loss
    val loss = model.messages.filter(_.contains("loss=")).map { line =>
      line.split(" ")(1).split("=").last.toFloat
    }

    for (i <- 0 until numEpoch - 1) {
      assert(loss(i + 1) < loss(i), s"loss increase: ${loss.mkString("->")}")
    }
  }

  test("second order") {
    param.setOrder(2)
    val model = new LINEModel(param) {
      val messages = new ArrayBuffer[String]()

      // mock logs
      override def logTime(msg: String): Unit = {
        if (null != messages) {
          messages.append(msg)
        }
        println(msg)
      }
    }
    model.train(data, param, "")
    model.save(output, 0)
    model.destroy()

    // extract loss
    val loss = model.messages.filter(_.contains("loss=")).map { line =>
      line.split(" ")(1).split("=").last.toFloat
    }

    for (i <- 0 until numEpoch - 1) {
      assert(loss(i + 1) < loss(i), s"loss increase: ${loss.mkString("->")}")
    }
  }

}
