package com.tencent.angel.spark.automl

import java.io.{FileOutputStream, PrintStream}

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.operator.{FtestSelector, LassoSelector, RandomForestSelector, VarianceSelector}
import org.junit.Test

class FeatureSelectorTest {

  val spark = SparkSession.builder().master("local").getOrCreate()

  val numTopFeatures = 50

  @Test def testLasso(): Unit = {
    System.setOut(new PrintStream(new FileOutputStream("lasso.out")))
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
      .setOutputCol("selectedFeatures")
      .setNumTopFeatures(numTopFeatures)

    val selectorModel = selector.fit(trainDF)

    val selectedTrainDF = selectorModel.transform(trainDF)
    val selectedTestDF = selectorModel.transform(testDF)

    val selectedLR = new LogisticRegression()
      .setMaxIter(20)
      .setFeaturesCol("selectedFeatures")

    val selectedAUC = selectedLR.fit(selectedTrainDF).evaluate(selectedTestDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"selected feature: auc = $selectedAUC")

    spark.stop()
  }

  @Test
  def testRandomForest(): Unit = {
    System.setOut(new PrintStream(new FileOutputStream("rf.out")))
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
      .setOutputCol("selectedFeatures")
      .setNumTopFeatures(numTopFeatures)

    val selectorModel = selector.fit(trainDF)

    val selectedTrainDF = selectorModel.transform(trainDF)
    val selectedTestDF = selectorModel.transform(testDF)

    val selectedLR = new LogisticRegression()
      .setMaxIter(20)
      .setFeaturesCol("selectedFeatures")

    val selectedAUC = selectedLR.fit(selectedTrainDF).evaluate(selectedTestDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"selected feature: auc = $selectedAUC")

    spark.stop()
  }

  @Test
  def testVariance(): Unit = {
    System.setOut(new PrintStream(new FileOutputStream("variance.out")))
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
      .setNumTopFeatures(numTopFeatures)

    val selectorModel = selector.fit(trainDF)

    val selectedTrainDF = selectorModel.transform(trainDF)
    val selectedTestDF = selectorModel.transform(testDF)

    val selectedLR = new LogisticRegression()
      .setMaxIter(20)
      .setFeaturesCol("selectedFeatures")

    val selectedAUC = selectedLR.fit(selectedTrainDF).evaluate(selectedTestDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"selected feature: auc = $selectedAUC")

    spark.stop()
  }

  @Test
  def testFtest(): Unit = {
    System.setOut(new PrintStream(new FileOutputStream("ftest.out")))
    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .option("vectorType", "dense")
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

    val selector = new FtestSelector()
      .setFeaturesCol("features")
      .setOutputCol("selectedFeatures")
      .setNumTopFeatures(numTopFeatures)

    val selectorModel = selector.fit(trainDF)

    val selectedTrainDF = selectorModel.transform(trainDF)
    val selectedTestDF = selectorModel.transform(testDF)

    val selectedLR = new LogisticRegression()
      .setMaxIter(20)
      .setFeaturesCol("selectedFeatures")

    val selectedAUC = selectedLR.fit(selectedTrainDF).evaluate(selectedTestDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"selected feature: auc = $selectedAUC")

    spark.stop()
  }

}
