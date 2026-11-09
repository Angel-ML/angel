package com.tencent.angel.spark.ml.time_series.timeModel

import com.tencent.angel.spark.ml.time_series.stat.TimeSeriesParams
import com.tencent.angel.spark.ml.util.{LagUtil, MatrixUtil}
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait AutoregressionParams extends TimeSeriesParams {
  final val maxLag = new Param[Int](this, "maxLag", "max lag")
  def setMaxLag(value: Int): this.type = set(maxLag, value)

  final val includeIntercept =
    new Param[Boolean](this, "includeIntercept", "intercept")
  def setIncludeIntercept(value: Boolean): this.type = set(includeIntercept, value)
}

class Autoregression(override val uid: String)
  extends Estimator[ARModel] with AutoregressionParams{

  def this() = this(Identifiable.randomUID("Autoregression"))

  setDefault(includeIntercept -> true, maxLag -> 1, timeCol -> "time", timeSeriesCol -> "timeseries")
  /**
    * Fits a model to the input data.
    */
  override def fit(dataset: Dataset[_]): ARModel = {
    val data = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
      // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()

    val dataVector = Vectors.dense(data.map(x => x._2))
    // Make left hand side
    val Y = MatrixUtil.toBreeze(dataVector)($(maxLag) until dataVector.size)
    // Make lagged right hand side
    val X = LagUtil.lagMatTrimBoth(dataVector, $(maxLag))

    val regression = new OLSMultipleLinearRegression()
    regression.setNoIntercept(!$(includeIntercept)) // drop intercept in regression
    regression.newSampleData(Y.toArray, MatrixUtil.matToRowArrs(X))
    val params = regression.estimateRegressionParameters()
    val (c, coeffs) = if (!$(includeIntercept)) (0.0, params) else (params.head, params.tail)

    new ARModel(c, coeffs)
      .setTimeCol($(timeCol))
      .setTimeSeriesCol($(timeSeriesCol))
  }

  override def copy(extra: ParamMap): Estimator[ARModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema
  }
}


class ARModel(override val uid: String, val c: Double, val coefficients: Array[Double]) extends
  Model[ARModel] with AutoregressionParams {

  def this(c: Double, coefficients: Array[Double]) = this(Identifiable.randomUID("ARModel"), c,
    coefficients)

  /**
    * Transforms the input dataset.
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    val data: Array[(Double, Double)] = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
      // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()
      .map(x => (x._1, x._2))

    val dataVector = Vectors.dense(data.map(x => x._2))

    val dest = addTimeDependentEffects(dataVector, dataVector)

    val res: Array[((Double, Double), Double)] = data.zip(dest.toArray)

    val resRDD = dataset.sparkSession.sparkContext.parallelize(
      res.map(r => Row(r._1._1, r._1._2, r._2)))

    val structType = transformSchema(dataset.schema)

    dataset.sparkSession.createDataFrame(resRDD, structType)
  }

  def removeTimeDependentEffects(ts: Vector,
                                 destTs: Vector = null): Vector = {
    val dest = if (destTs == null) new Array[Double](ts.size) else destTs.toArray
    var i = 0
    while (i < ts.size) {
      dest(i) = ts(i) - c
      var j = 0
      while (j < coefficients.length && i - j - 1 >= 0) {
        dest(i) -= ts(i - j - 1) * coefficients(j)
        j += 1
      }
      i += 1
    }
    new DenseVector(dest)
  }

  def addTimeDependentEffects(ts: Vector, destTs: Vector): Vector = {
    val dest = if (destTs == null) new Array[Double](ts.size) else destTs.toArray
    var i = 0
    while (i < ts.size) {
      dest(i) = c + ts(i)
      var j = 0
      while (j < coefficients.length && i - j - 1 >= 0) {
        dest(i) += dest(i - j - 1) * coefficients(j)
        j += 1
      }
      i += 1
    }
    new DenseVector(dest)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(StructField("date", DoubleType),
      StructField("label", DoubleType),
      StructField("prediction", DoubleType)))
  }

  override def copy(extra: ParamMap): ARModel = defaultCopy(extra)
}
