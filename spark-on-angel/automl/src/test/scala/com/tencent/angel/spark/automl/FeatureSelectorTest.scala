package com.tencent.angel.spark.automl

import com.tencent.angel.spark.automl.feature.select.{LassoSelector, RandomforestSelector, VarianceSelector}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class FeatureSelectorTest {

  val spark = SparkSession.builder().master("local").getOrCreate()

  @Test def testLasso(): Unit = {
    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val splitData = data.randomSplit(Array(0.7, 0.3))

    val selector = new LassoSelector()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")

    val trainDF = splitData(0)
    val testDF = splitData(1)

    val selectorModel = selector.fit(trainDF).setNumTopFeatures(50)

    val selectedTestDF = selectorModel.transform(testDF)

    selectedTestDF.show()

    spark.stop()

  }

  @Test
  def testRandomForest(): Unit = {

    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val splitData = data.randomSplit(Array(0.7, 0.3))

    val selector = new RandomforestSelector()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")

    val trainDF = splitData(0)
    val testDF = splitData(1)

    val selectorModel = selector.fit(trainDF).setNumTopFeatures(3)

    val selectedTestDF = selectorModel.transform(testDF)

    selectedTestDF.show()

    spark.stop()
  }

  @Test
  def testVariance(): Unit = {

    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val splitData = data.randomSplit(Array(0.7, 0.3))

    val selector = new VarianceSelector()
      .setFeaturesCol("features")
      .setOutputCol("selectedFeatures")

    val trainDF = splitData(0)
    val testDF = splitData(1)

    val selectorModel = selector.fit(trainDF).setNumTopFeatures(3)

    val selectedTestDF = selectorModel.transform(testDF)

    selectedTestDF.show()

    spark.stop()
  }

}
