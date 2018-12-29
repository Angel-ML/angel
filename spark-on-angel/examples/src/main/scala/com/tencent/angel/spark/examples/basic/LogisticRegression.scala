package com.tencent.angel.spark.examples.basic

import com.tencent.angel.ml.core.optimizer.Adam
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.util.DataLoader
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.ml.math2.vector.Vector

object LogisticRegression {

  def start(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("PSVector Examples")
    conf.set("spark.ps.model", "LOCAL")
    conf.set("spark.ps.jars", "")
    conf.set("spark.ps.instances", "1")
    conf.set("spark.ps.cores", "1")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def calculate(weight: Vector, data: Array[LabeledData]): Vector = {
    null
  }


  def main(args: Array[String]): Unit = {
    start()
    val input = "hdfs://"
    val dim = 10
    val data = SparkContext.getOrCreate().textFile(input).map(f => (DataLoader.parseIntFloat(f, dim)))

    val mat = PSMatrix.rand(4, 10, RowType.T_FLOAT_DENSE)
    mat.reset(Array(1, 2))
    val opt = new Adam(0.1)

    for (iteration <- 0 until 10) {
      mat.reset(3)
      val size = data.sample(false, 0.01, 42).mapPartitions {
        case iter =>
          PSContext.instance()
          val samples = iter.toArray
          // here we parse the indices of data to pull to pull the features we need.
          val weight = mat.pull(0, Array(0, 1))
          val gradient = calculate(weight, samples)
          mat.increment(3, gradient)
          Iterator.single(samples.length)
      }.reduce(_ + _)
      opt.update(mat.id, 1, iteration, size)
    }
  }

}
