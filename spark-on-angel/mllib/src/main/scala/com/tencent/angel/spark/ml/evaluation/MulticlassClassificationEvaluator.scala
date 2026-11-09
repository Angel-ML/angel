package com.tencent.angel.spark.ml.evaluation

import com.tencent.angel.spark.ml.util.{LogUtils, _}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator => MultiEvaluator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by isakjiang on 2022-05-05.
 */


object MulticlassClassificationEvaluator {

  def main(args: Array[String]): Unit = {
    LogUtils.logTime(s"Start ${this.getClass.getSimpleName} Processing...")
    val mode = "local"
    val input = "data/mlTestData/evaluation_multi.txt" //xxxx.txt,poker_hand.csv

    val sep = ","
    val sampleRate = 1
    val partitionNum = 1

    val labelCol = 0
    val scoreCol = 1


    // (supports `"f1"`(default), `"accuracy"`, `"weightedPrecision"`, `"weightedRecall"`)
    val metricName = "all"

    val modelPath = "data/output/aaa"

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
