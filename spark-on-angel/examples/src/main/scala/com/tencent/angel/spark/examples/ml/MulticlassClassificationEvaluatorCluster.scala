package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.util.{LogUtils, _}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator => MultiEvaluator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by isakjiang on 2022-05-05.
 */


object MulticlassClassificationEvaluatorCluster {

  def main(args: Array[String]): Unit = {
    LogUtils.logTime(s"Start ${this.getClass.getSimpleName} Processing...")
    val params = ArgsUtil.parse(args)

    val mode = params.getOrElse(ParamKeys.MODE, "yarn")
    val input = params.getOrElse(ParamKeys.INPUT, null)

    val labelCol = params.getOrElse(ParamKeys.LABEL_COL, MAX_INT_STR).toInt
    val scoreCol = params.getOrElse(ParamKeys.SCORE_COL, MAX_INT_STR).toInt

    val partitionNum = params.getOrElse(ParamKeys.PARTITION_NUM, "10").toInt
    val sampleRate = params.getOrElse(ParamKeys.SAMPLE_RATE, "1.0").toDouble


    // (supports `"f1"`(default), `"accuracy"`, `"weightedPrecision"`, `"weightedRecall"`)
    val metricName = params.getOrElse(ParamKeys.METRIC_NAME, "f1")

    val modelPath = params.getOrElse(ParamKeys.MODEL_PATH, null)

    val sep = params.getOrElse("sep",  "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    // spark config
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    // run evaluation
    val tableDF = DataLoader.loadTable(sc, input, partitionNum, sampleRate, defaultFeatExpr, sep)

    val labelAndScore = DataFrameUtil.selectColsAsDoubles(tableDF, Array(labelCol, scoreCol))
    val colNames = labelAndScore.columns

    val evaluator = new MultiEvaluator()
      .setLabelCol(colNames(0))
      .setPredictionCol(colNames(1))

    outPutResult(evaluator, metricName, modelPath)


    def outPutResult(evaluator: MultiEvaluator, metricName: String, modelPath: String): Unit = {

      val metricNameList = if (metricName.equals("all")) List("f1", "accuracy",  "weightedPrecision","weightedRecall") else List(metricName)
      val metaSummary = if (modelPath != null && modelPath != "") MetaSummary.initEvaluateMeta(modelPath) else null


      metricNameList.foreach { metricName =>
        val metricValue = evaluator.setMetricName(metricName).evaluate(labelAndScore)
        LogUtils.logTime(metricName + " " + metricValue)
        if (metaSummary != null) {
          metaSummary.logMeta(metricName, metricValue)
        }
      }
    }
    sc.stop()
  }
}
