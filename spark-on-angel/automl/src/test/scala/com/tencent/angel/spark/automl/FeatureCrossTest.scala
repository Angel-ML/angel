/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.spark.automl

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.{Interaction, VectorAssembler}
import org.apache.spark.ml.feature.operator.{SelfCartesian, VarianceSelector, VectorCartesian}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.junit.Test

class FeatureCrossTest {

  val spark = SparkSession.builder().master("local").getOrCreate()

  @Test def testInteraction(): Unit = {
    val data = Seq(
      Row(Vectors.dense(Array(1.0,2.0,3.0)), Vectors.dense(Array(2.0,3.0,4.0)), Vectors.dense(Array(3.0,4.0,5.0)))
    )

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup1 = new AttributeGroup("vec1", attrs.asInstanceOf[Array[Attribute]])
    val attrGroup2 = new AttributeGroup("vec2", attrs.asInstanceOf[Array[Attribute]])
    val attrGroup3 = new AttributeGroup("vec3", attrs.asInstanceOf[Array[Attribute]])

    val schema = Seq(
      StructField("vec1", VectorType, true, attrGroup1.toMetadata),
      StructField("vec2", VectorType, true, attrGroup2.toMetadata),
      StructField("vec3", VectorType, true, attrGroup3.toMetadata)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val interaction = new Interaction()
      .setInputCols(Array("vec1", "vec2", "vec3"))
      .setOutputCol("interactedCol")

    val interacted = interaction.transform(df)

    interacted.show(truncate = false)
  }

  @Test def testVectorCartesian(): Unit = {
    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("../../data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val cartesian = new VectorCartesian()
      .setInputCols(Array("features", "features"))
      .setOutputCol("cartesian_features")

    val assembler = new VectorAssembler()
      .setInputCols(Array("features", "cartesian_features"))
      .setOutputCol("assemble_features")

    val pipeline = new Pipeline()
      .setStages(Array(cartesian, assembler))

    val featureModel = pipeline.fit(data)
    val crossDF = featureModel.transform(data)
    crossDF.show(1, truncate = false)

    val splitData = crossDF.randomSplit(Array(0.9, 0.1))

    val trainDF = splitData(0).persist()
    val testDF = splitData(1).persist()

    // original features
    val originalLR = new LogisticRegression()
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setRegParam(0.01)
    val originalAUC = originalLR.fit(trainDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"original feature: auc = $originalAUC")

    // cartesian features
    val cartesianLR = new LogisticRegression()
      .setFeaturesCol("cartesian_features")
      .setMaxIter(10)
      .setRegParam(0.01)
    val cartesianAUC = cartesianLR.fit(trainDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"cartesian feature: auc = $cartesianAUC")

    // original features + cartesian features
    val assemblerLR = new LogisticRegression()
      .setFeaturesCol("assemble_features")
      .setLabelCol("label")
      .setMaxIter(10)
      .setRegParam(0.01)
    val assemblerAUC = assemblerLR.fit(trainDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"original feature + cartesian feature: auc = $assemblerAUC")

    spark.close()

  }

  @Test def testSelfCartesian(): Unit = {
    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("../../data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val cartesian = new SelfCartesian()
      .setInputCol("features")
      .setOutputCol("cartesian_features")

    val assembler = new VectorAssembler()
      .setInputCols(Array("features", "cartesian_features"))
      .setOutputCol("assemble_features")

    val pipeline = new Pipeline()
      .setStages(Array(cartesian, assembler))

    val featureModel = pipeline.fit(data)
    val crossDF = featureModel.transform(data)
    crossDF.show(1, truncate = false)

    val splitData = crossDF.randomSplit(Array(0.7, 0.3))

    val trainDF = splitData(0).persist()
    val testDF = splitData(1).persist()

    // original features
    val originalLR = new LogisticRegression()
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setRegParam(0.01)
    val originalAUC = originalLR.fit(trainDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"original feature: auc = $originalAUC")

    // cartesian features
    val cartesianLR = new LogisticRegression()
      .setFeaturesCol("cartesian_features")
      .setMaxIter(10)
      .setRegParam(0.01)
    val cartesianAUC = cartesianLR.fit(trainDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"cartesian feature: auc = $cartesianAUC")

    // original features + cartesian features
    val assemblerLR = new LogisticRegression()
      .setFeaturesCol("assemble_features")
      .setLabelCol("label")
      .setMaxIter(10)
      .setRegParam(0.01)
    val assemblerAUC = assemblerLR.fit(trainDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"original feature + cartesian feature: auc = $assemblerAUC")

    spark.close()
  }

  @Test def testThreeOrderCross(): Unit = {
    val data = spark.read.format("libsvm")
      .option("numFeatures", 8)
      .load("../../data/abalone/abalone_8d_train.libsvm")
      .persist()

    val cartesian = new VectorCartesian()
      .setInputCols(Array("features", "features"))
      .setOutputCol("f_f")

    val cartesian2 = new VectorCartesian()
      .setInputCols(Array("features", "f_f"))
      .setOutputCol("f_f_f")

    val assembler = new VectorAssembler()
      .setInputCols(Array("features", "f_f", "f_f_f"))
      .setOutputCol("assembled_features")

    val pipeline = new Pipeline()
      .setStages(Array(cartesian, cartesian2, assembler))

    val crossDF = pipeline.fit(data).transform(data).persist()
    crossDF.show(1)

    val twoOrderDim = crossDF.head().getAs[DenseVector]("f_f").toArray.size
    println(s"three order features size: $twoOrderDim")

    val threeOrderDim = crossDF.head().getAs[DenseVector]("f_f_f").toArray.size
    println(s"three order features size: $threeOrderDim")

    val assembledDim = crossDF.head().getAs[DenseVector]("assembled_features").toArray.size
    println(s"assembled features size: $assembledDim")
  }

}
