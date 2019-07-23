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

package org.apache.spark.ml.feature.operator

import breeze.linalg.{argsort, DenseVector => BDV}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.operator.RandomForestSelectorModel.RandomForestSelectorModelWriter
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol, HasOutputCol}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  * Params for [[RandomForestSelector]] and [[RandomForestSelectorModel]].
  */
private[feature] trait RandomForestSelectorParams extends Params
  with HasFeaturesCol with HasOutputCol with HasLabelCol {

  /**
    * Number of features that selector will select, ordered by descending feature importance. If the
    * number of features is less than numTopFeatures, then this will select all features.
    * Only applicable when selectorType = "numTopFeatures".
    * The default value of numTopFeatures is 50.
    *
    * @group param
    */
  final val numTopFeatures = new IntParam(this, "numTopFeatures",
    "Number of features that selector will select, ordered by descending feature importance. If the" +
      " number of features is < numTopFeatures, then this will select all features.",
    ParamValidators.gtEq(1))
  setDefault(numTopFeatures -> 50)

  def getNumTopFeatures: Int = $(numTopFeatures)

  /**
    * Percentile of features that selector will select, ordered by statistics value descending.
    * Only applicable when selectorType = "percentile".
    * Default value is 0.1.
    * @group param
    */
  final val percentile = new DoubleParam(this, "percentile",
    "Percentile of features that selector will select, ordered by feature importance.",
    ParamValidators.inRange(0, 1))
  setDefault(percentile -> 0.1)

  /**
    * The selector type of the RandomForestSelector.
    * Supported options: "numTopFeatures" (default), "percentile".
    * @group param
    */
  final val selectorType = new Param[String](this, "selectorType",
    "The selector type of the ChisqSelector. " +
      "Supported options: " + RandomForestSelector.supportedSelectorTypes.mkString(", "),
    ParamValidators.inArray[String](RandomForestSelector.supportedSelectorTypes))
  setDefault(selectorType -> RandomForestSelector.NumTopFeatures)

  def getSelectorType: String = $(selectorType)
}

/**
  * RandomForest feature selection, which selects features with high feature impotrance.
  * The selector supports different selection methods: `numTopFeatures`, `percentile`.
  *  - `numTopFeatures` chooses a fixed number of top features according to a chi-squared test.
  *  - `percentile` is similar but chooses a fraction of all features instead of a fixed number.
  * By default, the selection method is `numTopFeatures`, with the default number of top features
  * set to 50.
  */
class RandomForestSelector(override val uid: String)
  extends Estimator[RandomForestSelectorModel] with RandomForestSelectorParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("randomForestSelector"))

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setNumTopFeatures(value: Int): this.type = set(numTopFeatures, value)

  def setPercentile(value: Double): this.type = set(percentile, value)

  def setSelectorType(value: String): this.type = set(selectorType, value)

  override def fit(dataset: Dataset[_]): RandomForestSelectorModel = {
    val rf = new RandomForestClassifier()
      .setFeaturesCol(${featuresCol})
      .setLabelCol(${labelCol})

    val rfModel = rf.fit(dataset)

    val featureImportances: Array[Double] = rfModel.featureImportances.toArray

    val sortedIndices: Array[Int] = argsort.argsortDenseVector_Double(BDV(featureImportances)).toArray.reverse

    new RandomForestSelectorModel(uid, sortedIndices)
      .setFeaturesCol(${featuresCol})
      .setOutputCol(${outputCol})
      .setNumTopFeatures(${numTopFeatures})
  }

  override def transformSchema(schema: StructType): StructType = {
    val otherPairs = RandomForestSelector.supportedSelectorTypes.filter(_ != $(selectorType))
    otherPairs.foreach { paramName: String =>
      if (isSet(getParam(paramName))) {
        logWarning(s"Param $paramName will take no effect when selector type = ${$(selectorType)}.")
      }
    }
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  override def copy(extra: ParamMap): RandomForestSelector = defaultCopy(extra)
}


object RandomForestSelector extends DefaultParamsReadable[RandomForestSelector] {

  /** String name for `numTopFeatures` selector type. */
  private[spark] val NumTopFeatures: String = "numTopFeatures"

  /** String name for `percentile` selector type. */
  private[spark] val Percentile: String = "percentile"

  /** Set of selector types that ChiSqSelector supports. */
  val supportedSelectorTypes: Array[String] = Array(NumTopFeatures, Percentile)

  override def load(path: String): RandomForestSelector = super.load(path)
}


/**
  * Model fitted by [[RandomForestSelector]].
  */
class RandomForestSelectorModel(override val uid: String,
                            val selectedFeatures: Array[Int])
  extends Model[RandomForestSelectorModel] with RandomForestSelectorParams with MLWritable {

  private var filterIndices: Array[Int] = selectedFeatures.sorted

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setNumTopFeatures(value: Int): this.type = set(numTopFeatures, value)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    val newField = prepOutputField(schema)
    val outputFields = schema.fields :+ newField
    StructType(outputFields)
  }

  /**
    * Prepare the output column field, including per-feature metadata.
    */
  private def prepOutputField(schema: StructType): StructField = {
    val selector = selectedFeatures.toSet
    val origAttrGroup = AttributeGroup.fromStructField(schema($(featuresCol)))
    val featureAttributes: Array[Attribute] = if (origAttrGroup.attributes.nonEmpty) {
      origAttrGroup.attributes.get.zipWithIndex.filter(x => selector.contains(x._2)).map(_._1)
    } else {
      Array.fill[Attribute](selector.size)(NominalAttribute.defaultAttr)
    }
    val newAttributeGroup = new AttributeGroup($(outputCol), featureAttributes)
    newAttributeGroup.toStructField()
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    filterIndices = selectedFeatures.take(${numTopFeatures}).sorted
//    println(s"select ${filterIndices.size} features: ${filterIndices.mkString(",")}")

    // select function, select the top features order by lasso coefficients
    val select = udf { vector: Vector =>
      vector match {
        // for DenseVector, just select top features
        case dv: DenseVector =>
          val values: Array[Double] = dv.toArray
          for (i <- 0 until filterIndices(0)) values(i) = 0
          for (k <- 0 until filterIndices.size - 1) {
            for (i <- filterIndices(k) + 1 until filterIndices(k+1)) {
              values(i) = 0
            }
          }
          for (i <- filterIndices.last + 1 until values.size) values(i) = 0
          Vectors.dense(values)
        case sv: SparseVector =>
          val selectedPairs = sv.indices.zip(sv.values)
            .filter{ case (k, v) => filterIndices.contains(k) }
          Vectors.sparse(sv.size, selectedPairs.map(_._1), selectedPairs.map(_._2))
        case _ =>
          throw new IllegalArgumentException("Require DenseVector or SparseVector in spark.ml.linalg, but "
            + vector.getClass.getSimpleName + " is given.")
      }
    }

    dataset.withColumn(
      $(outputCol),
      select(col($(featuresCol))),
      MetadataTransformUtils.featureSelectionTransform(
        dataset.select(${featuresCol}).schema.fields.last, filterIndices, selectedFeatures.length).build())
  }

  override def copy(extra: ParamMap): RandomForestSelectorModel = {
    val copied = new RandomForestSelectorModel(uid, filterIndices)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new RandomForestSelectorModelWriter(this)
}


object RandomForestSelectorModel extends MLReadable[RandomForestSelectorModel] {

  private[RandomForestSelectorModel]
  class RandomForestSelectorModelWriter(instance: RandomForestSelectorModel) extends MLWriter {

    private case class Data(selectedFeatures: Seq[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.selectedFeatures.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class RandomForestSelectorModelReader extends MLReader[RandomForestSelectorModel] {

    private val className = classOf[RandomForestSelectorModel].getName

    override def load(path: String): RandomForestSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val data = sparkSession.read.parquet(path).select("selectedFeatures").head()
      val selectedFeatures = data.getAs[Seq[Int]](0).toArray
      val model = new RandomForestSelectorModel(metadata.uid, selectedFeatures)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[RandomForestSelectorModel] = new RandomForestSelectorModelReader

  override def load(path: String): RandomForestSelectorModel = super.load(path)
}