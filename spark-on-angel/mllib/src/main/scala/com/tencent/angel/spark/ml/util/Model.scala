package com.tencent.angel.spark.ml.util


import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.{Saveable, Loader}

/**
 * Created by kevinzwyou on 16-8-28.
 */

trait Model extends Serializable {
  var metricsSummary: MetricsSummary = _
  var modelMeta: MetaSummary = _

  def initSummary(modelPath: String): this.type = {
    println("init model summary")
    metricsSummary = MetricsSummary(modelPath)
    modelMeta = MetaSummary.initTrainMeta(modelPath)
    this
  }

  def save(modelPath: String)

  def predict(input: DataFrame): DataFrame

  def evaluate(testSet: DataFrame, evaluator: Evaluator): Double

  def export():PMMLExportable

  def summary(): String

  def toPMML(): String
}

object Model {
  final val modelDir: String = "model"
  def getModelDir(modelPath: String): String = {
    if (modelPath.endsWith("/model") || modelPath.endsWith("/model/")) {
      modelPath
    } else {
      modelPath + "/" + modelDir
    }
  }
}

/**
 * DefaultModel for ml model
 */
class MLModel(var model: MLWritable) extends Model {

  def this() = this(null)

  def initModel(m: MLWritable): this.type = {
    model = m
    this
  }

  def save(modelPath: String) = {
    model.write.overwrite().save(Model.getModelDir(modelPath))
  }

  def predict(input: DataFrame): DataFrame = {
    model.asInstanceOf[Transformer].transform(input)
  }

  def summary(): String = {
    "[ML] Summary.Info"
  }

  def evaluate(testSet: DataFrame, evaluator: Evaluator): Double = {
    evaluator.evaluate(predict(testSet))
  }

  def toPMML(): String = {
    "[ML] PMML"
  }

  override def export(): PMMLExportable = ???
}

object MLModel{
  def loadFrom[T <: MLWritable](modelPath:String, reader: MLReadable[T]) = {
    val model = reader.load(Model.getModelDir(modelPath)).asInstanceOf[MLWritable]
    new MLModel(model)
  }
}

/**
 * DefaultModel for mllib model
 */
class MLlibModel(var model: Saveable) extends Model {

  def this() = this(null)

  def initModel(m: Saveable): this.type = {
    model = m
    this
  }

  def save(modelPath: String) = {
    val modelDir = Model.getModelDir(modelPath)
    val sc = SparkContext.getOrCreate()
    val fsPath = new Path(modelDir)
    val fs = fsPath.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(fsPath)) {
      fs.delete(fsPath, true)
    }
    model.save(sc, modelDir)
  }

  def predict(input: DataFrame): DataFrame = {
    val mllibModel = model.asInstanceOf[ClassificationModel]

    val predictUDF = udf { (feature: Any) =>
      mllibModel.predict(feature.asInstanceOf[Vector])
    }
    input.withColumn(DFStruct.PROB, predictUDF(col(DFStruct.FEATURE)))
  }

  def evaluate(testSet: DataFrame, evaluator: Evaluator): Double = ???

  def summary(): String = {
    "[MLLib] summary.info"
  }

  def toPMML(): String = {
    "[MLLib] PMML"
  }

  override def export(): PMMLExportable = ???
}

object MLlibModel{
  def loadFrom[T <: Saveable](modelPath:String, loader: Loader[T]) ={
    val model = loader.load(SparkContext.getOrCreate(), Model.getModelDir(modelPath)).asInstanceOf[Saveable]
    new MLlibModel(model)
  }
}




