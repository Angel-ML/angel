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

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import breeze.linalg.argsort
import breeze.linalg.{DenseVector => BDV}
import Math.abs


class LassoSelectorWrapper {

}

class LassoSelector(override val uid: String) extends Estimator[LassoSelectorModel]{

  var featuresCol: String = _
  var outputCol: String = _
  var labelCol: String = _

  def this() = this(Identifiable.randomUID("lassoSelector"))

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

  override def fit(dataset: Dataset[_]): LassoSelectorModel = {

    val lr = new LogisticRegression()
      .setFeaturesCol(featuresCol)
      .setLabelCol(labelCol)
      .setElasticNetParam(1.0)
      .setMaxIter(10)

    val lrModel = lr.fit(dataset)

    val cofficients: Array[Double] = lrModel.coefficients.toArray.map(i => abs(i))

    val sortedIndices: Array[Int] = argsort.argsortDenseVector_Double(BDV(cofficients)).toArray.reverse

    new LassoSelectorModel(uid, sortedIndices).setInputCol(featuresCol).setOutputCol(outputCol)

  }

  override def copy(extra: ParamMap): Estimator[LassoSelectorModel] = {
    new LassoSelector(uid).setFeaturesCol(featuresCol).setOutputCol(outputCol).setLabelCol(labelCol)
  }
}

class LassoSelectorModel(override val uid: String,
                         val sortedIndices: Array[Int]) extends Model[LassoSelectorModel]{

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
      vector match {
        // for DenseVector, just select top features
        case v1: DenseVector =>
          val values: Array[Double] = sortedIndices.take(numTopFeatures) map orginalValues
          Vectors.dense(values)
        case v2: SparseVector =>
          val nonZeroIndices = vector.asInstanceOf[SparseVector].indices.filter(_ < numTopFeatures)
          val values: Array[Double] = sortedIndices.take(numTopFeatures).sorted map orginalValues
          val nonZeroValues: Array[Double] = nonZeroIndices map values
          Vectors.sparse(numTopFeatures, nonZeroIndices, nonZeroValues)

        case _ =>
          throw new IllegalArgumentException("Require DenseVector or SparseVector in spark.ml.linalg, but "
            + vector.getClass.getSimpleName + " is given.")
      }

    }
    dataset.withColumn(outputCol, select(col(inputCol)))
  }

  override def copy(extra: ParamMap): LassoSelectorModel = {
    new LassoSelectorModel(uid, sortedIndices)
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setNumTopFeatures(numTopFeatures)
  }
}
