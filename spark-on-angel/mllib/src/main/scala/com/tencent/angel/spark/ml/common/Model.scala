package com.tencent.angel.spark.ml.common

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import com.tencent.angel.spark.ml.util._

trait Model extends Serializable {
  /**
   * Save model to HDFS or LOCAL path.
   */
  def save(modelPath: String)

  /**
   * Predict a DataFrame data
   */
  def predict(input: DataFrame): DataFrame

  def evaluate(validateSet: DataFrame, evaluator: Evaluator): Double

  /**
   * During training the model, the loss, train AUC, validate AUC of each iteration will be
   * store as `summary`, which is a part of this model.
   */
  def summary(): String
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

  def save(modelPath: String): Unit = {
    model.write.overwrite().save(modelPath)
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
}

object MLModel{
  def loadFrom[T <: MLWritable](modelPath: String, reader: MLReadable[T]): Model = {
    val model = reader.load(modelPath).asInstanceOf[MLWritable]
    new MLModel(model)
  }
}

/**
 * DefaultModel for mllib model
 */
class MLlibModel(var model: Saveable) extends Model {

  def save(modelPath: String): Unit = {
    val sc = SparkContext.getOrCreate()
    val fsPath = new Path(modelPath)
    val fs = fsPath.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(fsPath)) fs.delete(fsPath, true)
    model.save(sc, modelPath)
  }

  def predict(input: DataFrame): DataFrame = {
    val mllibModel = model.asInstanceOf[ClassificationModel]

    val predictUDF = udf { (feature: Any) =>
      mllibModel.predict(feature.asInstanceOf[Vector])
    }
    input.withColumn(DFStruct.PROB, predictUDF(col(DFStruct.FEATURE)))
  }

  def evaluate(testSet: DataFrame, evaluator: Evaluator): Double = {
    0.5
  }

  def summary(): String = {
    "[MLLib] summary.info"
  }
}

object MLlibModel{
  def loadFrom[T <: Saveable](modelPath: String, loader: Loader[T]): Model = {
    val model = loader.load(SparkContext.getOrCreate(), modelPath).asInstanceOf[Saveable]
    new MLlibModel(model)
  }
}




