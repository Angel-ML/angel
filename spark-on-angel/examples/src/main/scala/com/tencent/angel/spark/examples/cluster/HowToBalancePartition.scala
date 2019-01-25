package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.ml.math2.vector.LongDummyVector
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.util.DataLoader
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object HowToBalancePartition {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val params = ArgsUtil.parse(args)
    val numPart = params.getOrElse("numPart", "800").toInt
    val input = params.getOrElse("input", "data/census/census_148d_train.libsvm")



    val freq = sc.textFile(input)
      .map(s => (DataLoader.parseLongDummy(s, -1))).flatMap(f => f.getX.asInstanceOf[LongDummyVector].getIndices)
//      .distinct()
      .map(f => f / 0xfffffffffL).map(f => (f, 1L)).reduceByKey(_ + _).collect()

    val sorted = freq.sortBy(f => f._1)

//    sorted.map(f => println(f))

    val sum = sorted.map(f => f._2).sum
    val per = sum / numPart

    val size = sorted.size
    var current = 0L
    val starts = new ArrayBuffer[Long]()
    starts.append(0L)
    for (i <- 0 until size) {
      if (current > per) {
        println(current)
        current = 0L
        starts.append(sorted(i)._1)
      }
      current += sorted(i)._2
    }

    println(starts.size)

    println(starts.map(f => f << 36).mkString("L,"))

  }

}
