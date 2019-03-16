package com.tencent.angel.spark.automl

import com.tencent.angel.spark.automl.feature.select.{LassoSelector, LassoSelectorModel, RandomForestSelector, VarianceSelector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.junit.Test

class FeatureSelectorTest {

  val spark = SparkSession.builder().master("local").getOrCreate()

  @Test def testLasso(): Unit = {
    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("../../data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val splitData = data.randomSplit(Array(0.7, 0.3))
    val trainDF = splitData(0)
    val testDF = splitData(1)

    val originalLR = new LogisticRegression()
      .setMaxIter(20)
      .setFeaturesCol("features")

    val originalAUC = originalLR.fit(trainDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"original feature: auc = $originalAUC")

    val selector = new LassoSelector()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")
      .setNumTopFeatures(50)

    val selectedDF = selector.fit(data).transform(data)

    val splitSelectedDF = selectedDF.randomSplit(Array(0.7, 0.3))
    val selectedTrainDF = splitSelectedDF(0)
    val selectedTestDF = splitSelectedDF(1)

    val selectedLR = new LogisticRegression()
      .setMaxIter(20)
      .setFeaturesCol("selectedFeatures")

    val selectedAUC = selectedLR.fit(selectedTrainDF).evaluate(selectedTestDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"selected feature: auc = $selectedAUC")

    spark.stop()
  }

  @Test
  def testRandomForest(): Unit = {

    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("../../data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val splitData = data.randomSplit(Array(0.7, 0.3))
    val trainDF = splitData(0)
    val testDF = splitData(1)

    val originalLR = new LogisticRegression()
      .setMaxIter(20)
      .setFeaturesCol("features")

    val originalAUC = originalLR.fit(trainDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"original feature: auc = $originalAUC")

    val selector = new RandomForestSelector()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")
      .setNumTopFeatures(50)

    val selectedDF = selector.fit(data).transform(data)

    val splitSelectedDF = selectedDF.randomSplit(Array(0.7, 0.3))
    val selectedTrainDF = splitSelectedDF(0)
    val selectedTestDF = splitSelectedDF(1)

    val selectedLR = new LogisticRegression()
      .setMaxIter(20)
      .setFeaturesCol("selectedFeatures")

    val selectedAUC = selectedLR.fit(selectedTrainDF).evaluate(selectedTestDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"selected feature: auc = $selectedAUC")

    spark.stop()
  }

  @Test
  def testVariance(): Unit = {

    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("../../data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val splitData = data.randomSplit(Array(0.7, 0.3))
    val trainDF = splitData(0)
    val testDF = splitData(1)

    val originalLR = new LogisticRegression()
      .setMaxIter(20)
      .setFeaturesCol("features")

    val originalAUC = originalLR.fit(trainDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"original feature: auc = $originalAUC")

    val selector = new VarianceSelector()
      .setFeaturesCol("features")
      .setOutputCol("selectedFeatures")
      .setNumTopFeatures(50)

    val selectedDF = selector.fit(data).transform(data)

    val splitSelectedDF = selectedDF.randomSplit(Array(0.7, 0.3))
    val selectedTrainDF = splitSelectedDF(0)
    val selectedTestDF = splitSelectedDF(1)

    val selectedLR = new LogisticRegression()
      .setMaxIter(20)
      .setFeaturesCol("selectedFeatures")

    val selectedAUC = selectedLR.fit(selectedTrainDF).evaluate(selectedTestDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"selected feature: auc = $selectedAUC")

    spark.stop()
  }

}
