package org.apache.spark.ml.feature.select

import com.tencent.angel.spark.ml.featurePreprocess.TreeBasedSelector.MetadataTransformUtils
import com.tencent.angel.spark.ml.util.{DefaultAngelParamsReader, DefaultAngelParamsWriter}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.functions._

import scala.collection.mutable


trait TreeBasedSelectorParams extends Params with
  HasFeaturesCol with HasOutputCol with HasLabelCol {
  final val numTopFeatures = new IntParam(this, "numTopFeatures",
    "Number of features that selector will select, ordered by ascending weights. If the" +
      " number of features is < numTopFeatures, then this will select all features.",
    ParamValidators.gtEq(1))
  setDefault(numTopFeatures -> 50)

  def getNumTopFeatures: Int = $(numTopFeatures)

  final val numTrees = new IntParam(this, "numTrees", "Number of trees to train (>= 1)",
    ParamValidators.gtEq(1))

  setDefault(numTrees -> 50)

  def getNumTrees: Int = $(numTrees)

  final val percentile = new DoubleParam(this, "percentile",
    "Percentile of features that selector will select, ordered by ascending weights.",
    ParamValidators.inRange(0, 1))
  setDefault(percentile -> 0.1)

  def getPercentile: Double = $(percentile)

  final val selectorType = new Param[String](this, "selectorType",
    "The selector type of the TreeBasedSelector. " +
      "Supported options: " + Array("numTopFeatures", "percentile"),
    ParamValidators.inArray[String](Array("numTopFeatures", "percentile")))

  setDefault(selectorType -> "numTopFeatures")

  def getSelectorType: String = $(selectorType)

  final val taskType = new Param[String](this, "taskType",
    "classifier or regressor",
    ParamValidators.inArray[String](Array("classifier", "regressor")))
}


final class TreeBasedSelector(override val uid: String)
  extends Estimator[TreeBasedSelectorModel]
    with TreeBasedSelectorParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("TreeBasedSelector"))

  /** @group setParam */
  def setNumTopFeatures(value: Int): this.type = set(numTopFeatures, value)

  /** @group setParam */
  def setPercentile(value: Double): this.type = set(percentile, value)

  /** @group setParam */
  def setSelectorType(value: String): this.type = set(selectorType, value)

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setTaskType(value: String): this.type = set(taskType, value)

  override def fit(dataset: Dataset[_]): TreeBasedSelectorModel = {
    transformSchema(dataset.schema, logging = true)

    val featureImportancesResult = $(taskType) match {
      case "classifier" =>
        new RandomForestClassifier()
          .setLabelCol($(labelCol))
          .setFeaturesCol($(featuresCol))
          .setNumTrees($(numTrees))
          .fit(dataset)
          .featureImportances.toArray.zipWithIndex
      case "regressor" =>
        new RandomForestRegressor()
          .setLabelCol($(labelCol))
          .setFeaturesCol($(featuresCol))
          .setNumTrees($(numTrees))
          .fit(dataset)
          .featureImportances.toArray.zipWithIndex
    }

    val features = $(selectorType) match {
      case TreeBasedSelector.NumTopFeatures =>
        featureImportancesResult.sortBy { case (res, _) => res}.take($(numTopFeatures))
      case TreeBasedSelector.Percentile =>
        featureImportancesResult.sortBy { case (res, _) => res}
          .take((featureImportancesResult.length * $(percentile)).toInt)
      case errorType =>
        throw new IllegalStateException(s"Unknown TreeBasedSelector Type: $errorType")
    }

    val indices = features.map { case (_, index) => index }

    copyValues(new TreeBasedSelectorModel(uid, indices))
  }

  override def transformSchema(schema: StructType): StructType = {
    val supportedSelectorTypes = Array("numTopFeatures", "percentile")
    val otherPairs = supportedSelectorTypes.filter(_ != $(selectorType))
    otherPairs.foreach { paramName: String =>
      if (isSet(getParam(paramName))) {
        logWarning(s"Param paramName will take no effect when selector type = $selectorType.")
      }
    }
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.checkNumericType(schema, $(labelCol))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  override def copy(extra: ParamMap): TreeBasedSelector = defaultCopy(extra)
}

object TreeBasedSelector extends DefaultParamsReadable[TreeBasedSelector] {
  override def load(path: String): TreeBasedSelector = super.load(path)
  val NumTopFeatures: String = "numTopFeatures"
  val Percentile: String = "percentile"
}

final class TreeBasedSelectorModel(override val uid: String,
                                   val selectedFeatures: Array[Int])
  extends Model[TreeBasedSelectorModel] with TreeBasedSelectorParams with MLWritable {

  import org.apache.spark.ml.feature.select.TreeBasedSelectorModel.TreeBasedSelectorModelWriter


  private val filterIndices = selectedFeatures.sorted

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val transformedSchema = transformSchema(dataset.schema, logging = true)
    val newField = transformedSchema.last
    val transformer: Vector => Vector = v => compress(v)

    val selector = udf(transformer)
    dataset.withColumn($(outputCol), selector(col($(featuresCol))), MetadataTransformUtils.featureSelectionTransform(
      dataset.select($(featuresCol)).schema.fields.last, filterIndices, selectedFeatures.length).build())
      .drop($(featuresCol))
  }

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    val newField = prepOutputField(schema)
    val outputFields = schema.fields :+ newField
    StructType(outputFields)
  }

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

  private def compress(features: Vector): Vector = {
    features match {
      case SparseVector(size, indices, values) =>
        val newSize = filterIndices.length
        val newValues = new mutable.ArrayBuilder.ofDouble
        val newIndices = new mutable.ArrayBuilder.ofInt
        var i = 0
        var j = 0
        var indicesIdx = 0
        var filterIndicesIdx = 0
        while (i < indices.length && j < filterIndices.length) {
          indicesIdx = indices(i)
          filterIndicesIdx = filterIndices(j)
          if (indicesIdx == filterIndicesIdx) {
            newIndices += j
            newValues += values(i)
            j += 1
            i += 1
          } else {
            if (indicesIdx > filterIndicesIdx) {
              j += 1
            } else {
              i += 1
            }
          }
        }
        Vectors.sparse(newSize, newIndices.result(), newValues.result())
      case DenseVector(values) =>
        val values = features.toArray
        Vectors.dense(filterIndices.map(i => values(i)))
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }

  override def copy(extra: ParamMap): TreeBasedSelectorModel = {
    val copied = new TreeBasedSelectorModel(uid, selectedFeatures)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new TreeBasedSelectorModelWriter(this)
}

object TreeBasedSelectorModel extends MLReadable[TreeBasedSelectorModel] {
  class TreeBasedSelectorModelWriter(instance: TreeBasedSelectorModel) extends MLWriter {
    private case class Data(selectedFeatures: Seq[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultAngelParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.selectedFeatures.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class TreeBasedSelectorModelReader extends MLReader[TreeBasedSelectorModel] {

    private val className = classOf[TreeBasedSelectorModel].getName

    override def load(path: String): TreeBasedSelectorModel = {
      val metadata = DefaultAngelParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("selectedFeatures").head()
      val selectedFeatures = data.getAs[Seq[Int]](0).toArray
      val model = new TreeBasedSelectorModel(metadata.uid, selectedFeatures)
      DefaultAngelParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[TreeBasedSelectorModel] = new TreeBasedSelectorModelReader

  override def load(path: String): TreeBasedSelectorModel = super.load(path)
}



