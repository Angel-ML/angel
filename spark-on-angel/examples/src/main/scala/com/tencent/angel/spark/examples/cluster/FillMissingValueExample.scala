package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.featureEngineering.FillMissingValue.FillMissingValueConf
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}

/**
 * Created by isakjiang on 2020-11-16
 * if user value to fill the missing feature,then all features use one value
 */

object FillMissingValueExample {

  val MISSING_VALUE = "missingValue"
  val MEAN = "mean"
  val MEDIAN = "median"
  val COUNT = "count"
  val FILL_STAT_PATH = "fillStatPath"
  val CONCAT_SYMBOL = " "

  def main(args: Array[String]): Unit = {


    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    // feature config to fill the missing value
    val fillConfPath = params.getOrElse("user-files", null)

    // this path is for save the fill method for each specified feature
    val fillStatPath = params.getOrElse("fillStatPath", null)

    val sc = start(mode)
    run(sc, input, output, partitionNum, sampleRate, fillConfPath, fillStatPath, sep)
    stop()
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          partitionNum: Int,
          sampleRate: Double,
          scaleConfPath: String,
          fillStatPath: String,
          sep: String) = {

    val fConf = FillMissingValueConf.parser(scaleConfPath)

    val inputDF = DataLoader.loadTableWithNull(sc, input, partitionNum, sampleRate, null, sep).rdd
      .map { case row: Row =>
        row.toSeq.toArray.map { x =>
          if (x != null && x != "null" && x != "" && x != "NULL") x.toString else null
        }
      }.cache()


    // the format is id, missingvalue, mean array or median array or count array
    val fillConf = fConf.map { x =>

      // feature id
      val id = x._1
      // whether exists missing value
      val missingValue = x._2
      // whether exists fill method
      val fillMethod = x._3

      // parse the feature ids
      val selectedCols = DataLoader
        .parseFeatureCols(id)
        .flatMap { case (begin, end) => begin to end }

      val fillVal = if (missingValue != null) {
        val missing4Ids = new Array[String](selectedCols.length).map(x => missingValue)
        (missing4Ids, MISSING_VALUE)
      }
      else {
        // parse the feature config
        fillMethod match {
          case MEAN =>
            (getMeanByID(inputDF, selectedCols), MEAN)
          case MEDIAN =>
            (getMedianByIDLarge(inputDF, selectedCols), MEDIAN)
          //(getMedianByID(inputDF, selectedCols), MEDIAN)
          case COUNT =>
            (getCountByID(inputDF, selectedCols), COUNT)
        }
      }
      (selectedCols, fillVal)
    }

    // save the information of fill
    if (fillStatPath != null) {
      val fillInfor = fillConf.map { arr =>
        val leg = arr._1.length
        val idFillInfor = (0 until leg).map(i => (arr._1(i) + ":" + arr._2._1(i))).mkString(CONCAT_SYMBOL)
        Array(arr._2._2 + CONCAT_SYMBOL + idFillInfor)
      }
      val filledInforRdd = sc.makeRDD(fillInfor, 1)
      DataSaver.save(filledInforRdd, fillStatPath, sep)
    }

    // integrate the feature configure
    val allConfigIds = fillConf.flatMap(x => x._1)
    val allConfigVals = fillConf.flatMap(x => x._2._1)

    // fill the null value for some feature
    val filledResult = inputDF.map(feature => fillMissingValue(feature, allConfigIds, allConfigVals))
    DataSaver.save(filledResult, output, sep)
    inputDF.unpersist()

  }

  // fill by a missing value
  def fillMissingValue(feature: Array[String], ids: Array[Int], fillValue: Array[String]): Array[String] = {

    (0 until ids.length).map { i =>
      val id = ids(i)
      val updateValue = fillValue(i)
      feature(id) = if (feature(id) != null) feature(id) else updateValue
    }
    feature
  }

  // get the mean of features according to the feature ids
  def getMeanByID(inputDF: RDD[Array[String]], selectedCols: Array[Int]): Array[String] = {

    val featSum = inputDF
      .map { arr =>
        val featureVec = selectedCols.map(id => (arr(id)))
        val valueCount = new Array[Int](featureVec.length).map(x => 1)
        (featureVec, valueCount)
      }
      .treeReduce { (arrCount1, arrCount2) =>
        val statOnId = (0 until selectedCols.length).map { i =>
          val arr1 = arrCount1._1
          val arr2 = arrCount2._1
          val count1 = arrCount1._2
          val count2 = arrCount2._2

          if ((arr1(i) != null) && (arr2(i) != null)) {
            val featureSum = (arr1(i).toDouble + arr2(i).toDouble).toString
            (featureSum, count1(i) + count2(i))
          } else if (arr1(i) != null) {
            (arr1(i), count1(i))
          } else if (arr2(i) != null) {
            (arr2(i), count2(i))
          } else {
            (arr1(i), 0)
          }
        }.toArray

        (statOnId.map(_._1), statOnId.map(_._2))
      }

    (0 until featSum._1.length).map { i =>
      val id = selectedCols(i)
      val sum = featSum._1(i)
      val count = featSum._2(i)
      if (sum == null)
        throw new Exception(s"Wrong config for mean at id:$id,all feature value is null")
      else
        (sum.toDouble / count).toString
    }.toArray
  }


  // get the median of feature according to the feature ids
  def getMedianByID(inputDF: RDD[Array[String]], selectedCols: Array[Int]): Array[String] = {

    selectedCols.map { id =>
      val medianValue = inputDF
        .map(x => x(id))
        .filter(x => x != null)
        .sortBy(x => x)
        .zipWithIndex()
        .collect()

      val medianLeg = medianValue.length
      if (medianLeg != 0) {
        val medianResult = medianValue.filter(x => x._2 == (medianLeg / 2))
        println("getMedianByID ori", medianResult(0)._1)
        medianResult(0)._1
      }
      else {
        throw new Exception(s"Wrong config for median at id:$id,all feature value is null")
      }
    }
  }

  // get the median of feature according to the feature ids
  def getMedianByIDLarge(inputDF: RDD[Array[String]], selectedCols: Array[Int]): Array[String] = {

    selectedCols.map { id =>
      val medianValue = inputDF
        .map(x => x(id))
        .filter(x => x != null)
        .sortBy(x => x)
        .zipWithIndex()
        .map { case (v, idx) => (idx, v) }

      val count = medianValue.count()

      if (count != 0) {
        val median: Double = if (count % 2 == 0) {
          val l = count / 2 - 1
          val r = l + 1
          (medianValue.lookup(l).head.toDouble + medianValue.lookup(r).head.toDouble) / 2
        } else medianValue.lookup(count / 2).head.toDouble
        println("getMedianByIDLarge", median)
        median.toString
      }
      else {
        throw new Exception(s"Wrong config for median at id:$id,all feature value is null")
      }
    }
  }


  // get the most count of the features according to the feature ids
  def getCountByID(inputDF: RDD[Array[String]], selectedCols: Array[Int]): Array[String] = {

    // each record refactor as (id, value, count)
    val countById = inputDF
      .flatMap(arr => selectedCols.map(id => ((id, arr(id)), 1)))
      .filter(x => x._1._2 != null)
      .reduceByKey(_ + _)
      .map(x => (x._1._1, (x._1._2, x._2)))
      .reduceByKey { (value1, value2) =>
        if (value1._2 > value2._2) value1 else value2
      }
      .map(x => (x._1, x._2._1))
      .collect()

    selectedCols.map(id => countById.toMap.getOrElse(id, null))
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
