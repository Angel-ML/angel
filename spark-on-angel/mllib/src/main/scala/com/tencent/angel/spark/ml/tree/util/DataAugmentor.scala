package com.tencent.angel.spark.ml.tree.util

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object DataAugmentor {

  def parseLibsvm(line: String, dim: Int): (Int, Array[Int], Array[Double]) = {
    val splits = line.split("\\s+|,").map(_.trim)
    val y = splits(0).toInt

    val indices = new Array[Int](splits.length - 1)
    val values = new Array[Double](splits.length - 1)
    for (i <- 0 until splits.length - 1) {
      val kv = splits(i + 1).split(":")
      indices(i) = kv(0).toInt
      values(i) = kv(1).toDouble
    }

    (y, indices, values)
  }

  def instanceAug(ratio: Int) = (ins: (Int, Array[Int], Array[Double])) => {
    val values = ins._3
    for (i <- values.indices)
      values(i) += Random.nextGaussian() * 0.1
    ins
  }

  def featureAug(numFeature: Int, ratio: Int) = (ins: (Int, Array[Int], Array[Double])) => {
    val indices = ins._2
    val values = ins._3
    val nnz = indices.length
    val augIndices = new Array[Int](nnz * ratio)
    val augValues = new Array[Double](nnz * ratio)
    for (i <- 0 until ratio) {
      for (j <- 0 until nnz) {
        augIndices(i * nnz + j) = indices(j) + numFeature * i
        augValues(i * nnz + j) = values(j) + Random.nextGaussian() * 0.1
      }
    }
    (ins._1, augIndices, augValues)
  }

  def insToString = (ins: (Int, Array[Int], Array[Double])) => {
    val sb = new StringBuilder(s"${ins._1}")
    val indices = ins._2
    val values = ins._3
    for (i <- indices.indices) {
      sb.append(f" ${indices(i)}:${values(i)}%.3f")
    }
    sb.toString()
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = SparkContext.getOrCreate(conf)
    val input = conf.get("spark.ml.input.path")
    val output = conf.get("spark.ml.output.path")
    val numWorker = conf.get("spark.ml.worker.num").toInt
    val numFeature = conf.get("spark.ml.feature.num").toInt
    val numInsScaleRatio = conf.getInt("spark.ml.instance.ratio", 1)
    val numFeatureScaleRatio = conf.getInt("spark.ml.feature.ratio", 1)

    val data = sc.textFile(input)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(line => parseLibsvm(line, numFeature))
      .repartition(numWorker)
      .map(featureAug(numFeature, numFeatureScaleRatio))
      .cache()
    println(s"Data count: ${data.count()}")

    for (i <- 0 until numInsScaleRatio) {
      println(s"Copy $i saving to $output/$i")
      data.map(instanceAug(numInsScaleRatio))
        .map(insToString)
        .saveAsTextFile(s"$output/$i")
    }
  }

}

