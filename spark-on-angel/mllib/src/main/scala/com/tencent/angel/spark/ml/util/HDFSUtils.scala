package com.tencent.angel.spark.ml.util

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.{Vectors => MLVectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object HDFSUtils {

  val columnNamePrefix = "f_"

  /**
   * read dense data from hdfs
   *
   */
  def readDenseData(sc: SparkContext,
                    path: String,
                    partitionNum: Int,
                    sampleRate: Double,
                    sep: String): DataFrame = {

    val inputRDD = sc.textFile(path, partitionNum)
      .sample(withReplacement = false, fraction = sampleRate)
      .map(line => line.trim.split(sep))

    val itemNum = inputRDD.first().length

    // create DataFrame
    val fields = ArrayBuffer[StructField]()
    for (i <- 0 until itemNum) {
      val name = columnNamePrefix + i
      val field = StructField(name, StringType, nullable = false)
      fields += field
    }

    val ss = SparkSession.builder().getOrCreate()
    val result = ss.createDataFrame(inputRDD.map(line => Row.fromSeq(line)), StructType(fields))

    result
  }


  /**
   * read dense data from hdfs;the value can be null
   *
   */
  def readDenseDataWithNull(sc: SparkContext,
                            path: String,
                            partitionNum: Int,
                            sampleRate: Double,
                            sep: String): DataFrame = {

    val inputRDD = sc.textFile(path, partitionNum)
      .sample(withReplacement = false, fraction = sampleRate)
      .map { line =>
        val features = line.split(sep, -1)
        val featureAfter = features.map { x =>
          if (x != "") x else null
        }
        featureAfter
      }

    val itemNum = inputRDD.first().length

    // create DataFrame
    val fields = ArrayBuffer[StructField]()
    for (i <- 0 until itemNum) {
      val name = columnNamePrefix + i
      val field = StructField(name, StringType, nullable = true)
      fields += field
    }

    val ss = SparkSession.builder().getOrCreate()
    val result = ss.createDataFrame(inputRDD.map(line => Row.fromSeq(line)), StructType(fields))

    result
  }


  def readByLine(sc: SparkContext, path: String, partitionNum: Int, sampleRate: Double): DataFrame = {
    val inputRDD = sc.textFile(path, partitionNum)
      .sample(withReplacement = false, fraction = sampleRate)
      .repartition(partitionNum)

    val stField = StructField(columnNamePrefix + 0, StringType, nullable = false)
    val ss = SparkSession.builder().getOrCreate()
    val result = ss.createDataFrame(inputRDD.map(line => Row(line)), StructType(Array(stField)))
    result
  }


  /**
   * read libsvm data from hdfs.
   *
   */
  def readLibsvmData(sc: SparkContext,
                     path: String,
                     partitionNum: Int,
                     sampleRate: Double,
                     dim: Int,
                     label: Int,
                     sep: String): DataFrame = {
    val parseRDD = sc.textFile(path)
      .sample(withReplacement = false, fraction = sampleRate)
      .repartition(partitionNum)
      .map(line => line.trim)
      .filter(_.nonEmpty)
      .map(line => parseLine(sep, line)).cache

    val d = if (dim > 0) {
      dim
    } else {
      parseRDD.map(record => record._2.lastOption.getOrElse(0))
        .reduce(math.max) + 1
    }

    val outputRDD = parseRDD.map { case (label_, indices, values) =>
      Row(label_, MLVectors.sparse(d, indices, values))
    }

    // label == -1 means DataLoader loads unlabeled data
    // else means it loads labeled data.
    val ss = SparkSession.builder().getOrCreate()

    if (label != -1) {
      val labeledRDD = outputRDD.map { row =>
        Row(row(0).toString.toDouble, row(1))
      }
      ss.createDataFrame(labeledRDD, LIBSVM_ST_ML)
    } else {
      ss.createDataFrame(outputRDD, LIBSVM_PREDICT_ST_ML)
    }
  }

  def save(dataRDD: RDD[Array[String]], path: String, sep: String): Unit = {
    val outputPath = new Path(path)
    val sc = dataRDD.context
    val conf = sc.hadoopConfiguration

    val fs = outputPath.getFileSystem(conf)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }
    dataRDD.map(_.mkString(sep)).saveAsTextFile(path)
  }


  private def parseLine(sep: String, line: String): (String, Array[Int], Array[Double]) = {

    val indices = ArrayBuffer[Int]()
    val values = ArrayBuffer[Double]()
    val items = line.split(sep).map(_.trim)
    val label = items.head

    for (item <- items.tail) {
      val ids = item.split(":")
      if (ids.length == 2) {
        indices += ids(0).toInt - 1 // convert 1-based indices to 0-based indices
        values += ids(1).toDouble
      }
      // check if indices are one-based and in ascending order
      var previous = -1
      var i = 0
      val indicesLength = indices.length
      while (i < indicesLength) {
        val current = indices(i)
        require(current > previous, "indices should be one-based and in ascending order")
        previous = current
        i += 1
      }
    }
    (label, indices.toArray, values.toArray)
  }


  def isSparse(sc: SparkContext, path: String, sep: String): Boolean = {
    val items = sc.textFile(path).first()
    for (item <- items.split(sep)) {
      if (item.matches("\\d+\\:.*")) {
        return true
      }
    }
    false
  }

}
