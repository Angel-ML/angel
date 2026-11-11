package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.util.{ArgsUtil, DataLoader, DataSaver, ParamKeys}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}

/**
 * Created by isakjiang on 2022/11/02.
 */
object OneHotEncoderCluster {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val featureCols = params.getOrElse("featureCols", null)
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    val sc = start(mode)
    run(sc, input, output, featureCols, partitionNum, sampleRate, sep)
    stop()

  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          featureCols: String,
          partitionNum: Int,
          sampleRate: Double,
          sep: String): Unit = {

    var data = DataLoader.loadTable(sc, input, partitionNum, sampleRate, null, sep)

    val featureSections = ArgsUtil.parseFeatureCols(featureCols)
    val inputCols = featureSections.flatMap { case (begin, end) =>
      data.columns.slice(begin, end + 1)
    }

    inputCols.foreach { colName =>
      data = data.withColumn(colName, col(colName).cast(DoubleType))
    }

    val outputCols = inputCols.map(i => i + "_onehot")

    val oneHotEncoder = new OneHotEncoder()
      .setInputCols(inputCols)
      .setDropLast(false)
      .setOutputCols(outputCols)

    var resultDF = oneHotEncoder.fit(data).transform(data).drop(inputCols: _*)

    outputCols.foreach { colName =>
      val size = resultDF.select(colName).head().get(0).asInstanceOf[SparseVector].size
      val outputColNames = (0 until size).map(i => colName + "_" + i.toString).toArray
      resultDF = new VectorDisassembler()
        .setInputCol(colName)
        .setOutputCols(outputColNames)
        .transform(resultDF).drop(colName)
    }
    resultDF.cache()

    println("the examples of final result  is ")
    resultDF.show(3)
    DataSaver.save(resultDF, output, sep)

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
