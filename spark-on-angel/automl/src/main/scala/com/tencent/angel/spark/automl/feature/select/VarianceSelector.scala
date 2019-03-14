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


package com.tencent.angel.spark.automl.feature.select

import breeze.linalg.{DenseVector, argsort}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{Vector => OldVector}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.StructType

class VarianceSelector(override val uid: String) extends Estimator[VarianceSelectorModel] {

  var featuresCol: String = _
  var outputCol: String = _

  def this() = this(Identifiable.randomUID("varianceSelector"))

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  def setFeaturesCol(value: String): this.type = {
    featuresCol = value
    this
  }

  def setOutputCol(value: String): this.type = {
    outputCol = value
    this
  }

  override def fit(dataset: Dataset[_]): VarianceSelectorModel = {
    val featuresRDD: RDD[OldVector]  = dataset.select(featuresCol).rdd.map{case Row(v: Vector) =>
      OldVectors.dense(v.toArray)
    }
    val summary: MultivariateStatisticalSummary = Statistics.colStats(featuresRDD)
    val variance: Array[Double] = summary.mean.toArray
    val sortedIndices: Array[Int] = argsort.argsortDenseVector_Double(DenseVector(variance)).toArray.reverse

    new VarianceSelectorModel(uid, sortedIndices).setInputCol(featuresCol).setOutputCol(outputCol)
  }

  override def copy(extra: ParamMap): Estimator[VarianceSelectorModel] = {
    new VarianceSelector().setFeaturesCol(featuresCol).setOutputCol(outputCol)
  }

}

class VarianceSelectorModel(override val uid: String,
                            val sortedIndices: Array[Int]) extends Model[VarianceSelectorModel] {

  var inputCol: String = _

  var outputCol: String = _

  var numTopFeatures: Int = 50

  def setNumTopFeatures(value: Int): this.type = {
    numTopFeatures = value
    this
  }

  def setInputCol(value: String): this.type = {
    inputCol = value
    this
  }

  def setOutputCol(value: String): this.type = {
    outputCol = value
    this
  }

  override def transformSchema(schema: StructType): StructType = schema

  override def transform(dataset: Dataset[_]): DataFrame = {
    sortedIndices.foreach(index => print(index + ", "))
    println()
    // select function, select the top features order by lasso cofficients
    val select = udf { vector: Vector =>
      val orginalValues: Array[Double] = vector.toArray
      val values: Array[Double] = sortedIndices.take(numTopFeatures) map orginalValues
      Vectors.dense(values)
    }
    dataset.withColumn(outputCol, select(col(inputCol)))
  }

  override def copy(extra: ParamMap): VarianceSelectorModel = {
    new VarianceSelectorModel(uid, sortedIndices)
      .setNumTopFeatures(numTopFeatures)
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }
}
