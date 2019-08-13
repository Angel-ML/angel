package com.tencent.angel.spark.ml.embedding

import com.tencent.angel.spark.ml.{PSFunSuite, SharedPSContext}
import com.tencent.angel.spark.ml.embedding.line2.LINEModel
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class LINEModelSuite2 extends PSFunSuite with SharedPSContext {
  val input = "../../data/bc/edge"
  val output = "file:///E://model_new/"
  val oldOutput = null//"file:///E:\\temp\\application_1565577700269_-1030306181_2e9fbfff-8f3c-41f4-b015-59490ef6daf5\\snapshot\\2"
  val tmpPath = "file:///E://temp"
  val numPartition = 1
  val lr = 0.025f
  val dim = 4
  val batchSize = 1024
  val numPSPart = 1
  val numEpoch = 5
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
    param.setModelCPInterval(1)
    param.setModelSaveInterval(1)
  }

  /*test("first order") {
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
    //model.destroy()

    // extract loss
    val loss = model.messages.filter(_.contains("loss=")).map { line =>
      line.split(" ")(1).split("=").last.toFloat
    }

    for (i <- 0 until numEpoch - 1) {
      assert(loss(i + 1) < loss(i), s"loss increase: ${loss.mkString("->")}")
    }
  }*/

  test("seconds order") {

    /*val path = new Path(output, "test")
    val fs = path.getFileSystem(new Configuration())
    val stream = fs.create(path, true, 10000000)

    var startTs = System.currentTimeMillis()
    for(i <- 0 until 10000000) {
      stream.writeFloat(i.toFloat)
    }
    println(s"write int use time=${System.currentTimeMillis() - startTs}")

    startTs = System.currentTimeMillis()
    for(i <- 0 until 10000000) {
      stream.writeFloat(i.toFloat)
    }
    println(s"write int use time=${System.currentTimeMillis() - startTs}")

    startTs = System.currentTimeMillis()
    val data = new Array[Byte](10000000 * 4)
    for(i <- 0 until 10000000) {
      val intValue = java.lang.Float.floatToIntBits(i)
      val index = i << 2
      data(index + 0) = ((intValue >>> 24) & 0xFF).toByte
      data(index + 1) = ((intValue >>> 16) & 0xFF).toByte
      data(index + 2) = ((intValue >>> 8) & 0xFF).toByte
      data(index + 3) = ((intValue) & 0xFF).toByte
    }
    stream.write(data)
    println(s"write int use time=${System.currentTimeMillis() - startTs}")
    */

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

    if(oldOutput == null) {
      model.randomInitialize(param.seed)
    } else {
      model.load(oldOutput)
    }

    model.train(data, param, output)
    model.save(output, param.numEpoch)
    //model.destroy()

    // extract loss
    val loss = model.messages.filter(_.contains("loss=")).map { line =>
      line.split(" ")(1).split("=").last.toFloat
    }

    for (i <- 0 until numEpoch - 1) {
      assert(loss(i + 1) < loss(i), s"loss increase: ${loss.mkString("->")}")
    }


  }
}
