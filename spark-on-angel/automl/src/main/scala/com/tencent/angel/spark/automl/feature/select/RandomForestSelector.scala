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

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.linalg.{Vector, Vectors, DenseVector, SparseVector}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import breeze.linalg.argsort
import breeze.linalg.{DenseVector => BDV}

import scala.util.Sorting


class RandomForestSelector(override val uid: String) extends Estimator[RandomForestSelectorModel]{

  var featuresCol: String = _
  var outputCol: String = _
  var labelCol: String = _
  var numTopFeatures: Int = _

  def this() = this(Identifiable.randomUID("RandomForestSelector"))

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

  def setLabelCol(value: String): this.type = {
    labelCol = value
    this
  }

  def setNumTopFeatures(value: Int): this.type = {
    numTopFeatures = value
    this
  }

  override def fit(dataset: Dataset[_]): RandomForestSelectorModel = {

    val rf = new RandomForestClassifier()
      .setFeaturesCol(featuresCol)
      .setLabelCol(labelCol)

    val rfModel = rf.fit(dataset)

    val featureImportances: Array[Double] = rfModel.featureImportances.toArray

    val sortedIndices: Array[Int] = argsort.argsortDenseVector_Double(BDV(featureImportances)).toArray.reverse

    new RandomForestSelectorModel(uid, sortedIndices)
      .setInputCol(featuresCol)
      .setOutputCol(outputCol)
      .setNumTopFeatures(numTopFeatures)

  }

  override def copy(extra: ParamMap): RandomForestSelector = {
    new RandomForestSelector(uid)
      .setFeaturesCol(featuresCol)
      .setOutputCol(outputCol)
      .setLabelCol(labelCol)
      .setNumTopFeatures(numTopFeatures)
  }
}

class RandomForestSelectorModel(override val uid: String,
                                val sortedIndices: Array[Int]) extends Model[RandomForestSelectorModel]{

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

    val selectedIndices: Array[Int] = sortedIndices.take(numTopFeatures)
    Sorting.quickSort(selectedIndices)
    println(s"selected indices: ${selectedIndices.mkString(",")}")

    // select function, select the top features order by feture importrances of random forest
    val select = udf { vector: Vector =>
      vector match {
        case dv: DenseVector =>
          val values: Array[Double] = dv.toArray
          for (i <- 0 until selectedIndices(0)) values(i) = 0
          for (k <- 0 until selectedIndices.size - 1) {
            for (i <- selectedIndices(k) + 1 until selectedIndices(k+1)) {
              values(i) = 0
            }
          }
          for (i <- selectedIndices.last + 1 until values.size) values(i) = 0
          Vectors.dense(values)
        case sv: SparseVector =>
          val selectedPairs = sv.indices.zip(sv.values)
            .filter{ case (k, v) => selectedIndices.contains(k) }
          Vectors.sparse(sv.size, selectedPairs.map(_._1), selectedPairs.map(_._2))
        case _ =>
          throw new IllegalArgumentException("Require DenseVector or SparseVector in spark.ml.linalg, but "
            + vector.getClass.getSimpleName + " is given.")
      }
    }
    dataset.withColumn(outputCol, select(col(inputCol)))
  }

  override def copy(extra: ParamMap): RandomForestSelectorModel = {
    new RandomForestSelectorModel(uid, sortedIndices)
      .setNumTopFeatures(numTopFeatures)
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setNumTopFeatures(numTopFeatures)
  }
}