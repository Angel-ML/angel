package com.tencent.angel.spark.examples.graph

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object InitGraphExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val actionType = params.getOrElse("actionType", "train").toString
    val input = params.getOrElse("input", "data/bc/edge").toString
    val initBatchSize = params.getOrElse("initBatchSize", "1000000").toInt
    val psPartNum = params.getOrElse("psPartNum", "1000").toInt
    val dataPartNum = params.getOrElse("dataPartNum", "10000").toInt
    val batchItemNum = params.getOrElse("batchItemNum", "100000").toInt
    val processNum = params.getOrElse("processNum", "10000").toInt
    val sampleCount = params.getOrElse("sampleCount", "10").toInt
    val maxNodeId = params.getOrElse("maxNodeId", "282049660").toInt
    val numEdge = params.getOrElse("numEdge", "9736309911").toLong

    val storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
    val sep = params.getOrElse("data.sep", "\t").toString

    println(s"input=${input} initBatchSize=${initBatchSize} psPartNum=${psPartNum} dataPartNum=${dataPartNum}")

    val conf = new SparkConf().setMaster("yarn-cluster")
    val sc = new SparkContext(conf)
    PSContext.getOrCreate(sc)

    val startTs = System.currentTimeMillis()
    val data = sc.textFile(input).mapPartitions { iter =>
      val r = new Random()
      iter.map { line =>
        val arr = line.split(sep)
        val src = arr(0).toInt
        val dst = arr(1).toInt
        (r.nextInt, (src, dst))
      }
    }.coalesce(dataPartNum).values.persist(storageLevel)
    //val numEdge = data.count()
    //val maxNodeId = data.map { case (src, dst) => math.max(src, dst) }.max().toLong + 1
    println(s"parse data use time = ${System.currentTimeMillis() - startTs} numEdge=$numEdge maxNodeId=$maxNodeId")

    val param = new Param()
    param.initBatchSize = initBatchSize
    param.maxIndex = maxNodeId.toInt
    param.psPartNum = psPartNum

    val neighborTable = new NeighborTable(param)
    neighborTable.initNeighbor(data, param)

    neighborTable.sampleNeighborsTest(batchItemNum, processNum, sampleCount)
    neighborTable.sampleNeighborsTest(data, batchItemNum, processNum, sampleCount)
  }
}
