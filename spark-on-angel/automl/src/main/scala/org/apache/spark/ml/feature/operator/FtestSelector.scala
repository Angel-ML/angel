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

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.functions._
import breeze.linalg.argsort
import breeze.linalg.{DenseVector => BDV, Vector => BV}
import breeze.stats.mean
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.ml.feature.operator.FtestSelectorModel.FtestSelectorModelWriter
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol, HasOutputCol}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD


/**
  * Params for [[FtestSelector]] and [[FtestSelectorModel]].
  */
private[feature] trait FtestSelectorParams extends Params
  with HasFeaturesCol with HasOutputCol with HasLabelCol {

  /**
    * Number of features that selector will select, ordered by descending lasso cofficients. If the
    * number of features is less than numTopFeatures, then this will select all features.
    * Only applicable when selectorType = "numTopFeatures".
    * The default value of numTopFeatures is 50.
    *
    * @group param
    */
  final val numTopFeatures = new IntParam(this, "numTopFeatures",
    "Number of features that selector will select, ordered by descending lasso cofficients. If the" +
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
    "Percentile of features that selector will select, ordered by lasso cofficients.",
    ParamValidators.inRange(0, 1))
  setDefault(percentile -> 0.1)

  /**
    * The selector type of the FtestSelector.
    * Supported options: "numTopFeatures" (default), "percentile".
    * @group param
    */
  final val selectorType = new Param[String](this, "selectorType",
    "The selector type of the ChisqSelector. " +
      "Supported options: " + FtestSelector.supportedSelectorTypes.mkString(", "),
    ParamValidators.inArray[String](FtestSelector.supportedSelectorTypes))
  setDefault(selectorType -> FtestSelector.NumTopFeatures)

  def getSelectorType: String = $(selectorType)
}


/**
  * Ftest feature selection, which selects features with high cofficients of the trained Logistic Regression Model.
  * The selector supports different selection methods: `numTopFeatures`, `percentile`.
  *  - `numTopFeatures` chooses a fixed number of top features according to a chi-squared test.
  *  - `percentile` is similar but chooses a fraction of all features instead of a fixed number.
  * By default, the selection method is `numTopFeatures`, with the default number of top features
  * set to 50.
  */
class FtestSelector(override val uid: String)
  extends Estimator[FtestSelectorModel] with FtestSelectorParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("FtestSelector"))

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setNumTopFeatures(value: Int): this.type = set(numTopFeatures, value)

  def setPercentile(value: Double): this.type = set(percentile, value)

  def setSelectorType(value: String): this.type = set(selectorType, value)

  /**
    * The function is implemented according to the f_oneway function in scipy
    */
  private def fOneWay(data: RDD[LabeledPoint]): Array[Double] = {

    def sumOfSquares(rdd: RDD[OldVector], nFeatures: Int): BV[Double] = {
      val squares: RDD[OldVector] = rdd.map{ v =>
        val bv = v.asBreeze
        OldVectors.fromBreeze(bv.map(i => i * i))
      }
      squares.reduce((v1, v2) => OldVectors.fromBreeze(v1.asBreeze + v2.asBreeze)).asBreeze
    }

    def squareOfSums(rdd: RDD[OldVector], nFeatures: Int): BV[Double] = {
      val sum: BV[Double] = rdd.reduce((v1, v2) => OldVectors.fromBreeze(v1.asBreeze + v2.asBreeze)).asBreeze
      sum.map(i => i * i)
    }


    val nClasses = data.map(point => point.label).collect().toSet.size
    val nSamples = data.count()
    val args = new Array[RDD[OldVector]](nClasses)
    for (label <- 0 until nClasses) {
      args(label) = data.filter(point => point.label == label.toDouble).map(point => point.features)
    }
    var allData: RDD[OldVector] = data.map(point => point.features)
    val summary: MultivariateStatisticalSummary = Statistics.colStats(allData)
    val offset = mean(summary.mean.asBreeze)

    allData = allData.map(v => OldVectors.fromBreeze(v.asBreeze - offset))

    val nFeatures = allData.take(1)(0).size

    val sstot = sumOfSquares(allData, nFeatures) - squareOfSums(allData, nFeatures) / nSamples.toDouble
//    println("ssot = \n" + sstot.toArray.mkString(","))

    var ssbn = BV.zeros[Double](nFeatures)
    for (arg <- args) {
      ssbn += squareOfSums(arg.map(v => OldVectors.fromBreeze(v.asBreeze - offset)), nFeatures) / arg.count().toDouble
    }
    ssbn -= squareOfSums(allData, nFeatures) / nSamples.toDouble
//    println("ssbn:\n" + ssbn.toArray.mkString(", "))

    val sswn: BV[Double] = sstot - ssbn

    val dfbn = nClasses - 1
    val dfwn = nSamples - nClasses
    val msb = ssbn / dfbn.toDouble
    val msw = sswn / dfwn.toDouble

    val fValue = msb / msw

    fValue.toArray
  }

  override def fit(dataset: Dataset[_]): FtestSelectorModel = {

    val data: RDD[LabeledPoint] = dataset.rdd.map{ case Row(label: Double, v: Vector) =>
      LabeledPoint(label, OldVectors.fromML(v))
    }

    val fValues = fOneWay(data)
//    println("fValues:\n" + fValues.mkString(", "))

    val sortedIndices: Array[Int] = argsort.argsortDenseVector_Double(BDV(fValues)).toArray.reverse

    new FtestSelectorModel(uid, sortedIndices)
      .setFeaturesCol(${featuresCol})
      .setOutputCol(${outputCol})
      .setNumTopFeatures(${numTopFeatures})
  }

  override def transformSchema(schema: StructType): StructType = {
    val otherPairs = FtestSelector.supportedSelectorTypes.filter(_ != $(selectorType))
    otherPairs.foreach { paramName: String =>
      if (isSet(getParam(paramName))) {
        logWarning(s"Param $paramName will take no effect when selector type = ${$(selectorType)}.")
      }
    }
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  override def copy(extra: ParamMap): FtestSelector = defaultCopy(extra)
}

object FtestSelector extends DefaultParamsReadable[FtestSelector] {

  /** String name for `numTopFeatures` selector type. */
  private[spark] val NumTopFeatures: String = "numTopFeatures"

  /** String name for `percentile` selector type. */
  private[spark] val Percentile: String = "percentile"

  /** Set of selector types that FtestSelector supports. */
  val supportedSelectorTypes: Array[String] = Array(NumTopFeatures, Percentile)

  override def load(path: String): FtestSelector = super.load(path)
}

/**
  * Model fitted by [[FtestSelector]].
  */
class FtestSelectorModel(override val uid: String,
                         val selectedFeatures: Array[Int])
  extends Model[FtestSelectorModel] with FtestSelectorParams with MLWritable {

  private var filterIndices: Array[Int] = selectedFeatures.take(${numTopFeatures}).sorted

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
    dataset.withColumn($(outputCol), select(col($(featuresCol))))
  }


  override def copy(extra: ParamMap): FtestSelectorModel = {
    val copied = new FtestSelectorModel(uid, filterIndices)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new FtestSelectorModelWriter(this)

}

object FtestSelectorModel extends MLReadable[FtestSelectorModel] {

  private[FtestSelectorModel]
  class FtestSelectorModelWriter(instance: FtestSelectorModel) extends MLWriter {

    private case class Data(selectedFeatures: Seq[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.selectedFeatures.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class FtestSelectorModelReader extends MLReader[FtestSelectorModel] {

    private val className = classOf[FtestSelectorModel].getName

    override def load(path: String): FtestSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("selectedFeatures").head()
      val selectedFeatures = data.getAs[Seq[Int]](0).toArray
      val model = new FtestSelectorModel(metadata.uid, selectedFeatures)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[FtestSelectorModel] = new FtestSelectorModelReader

  override def load(path: String): FtestSelectorModel = super.load(path)
}
