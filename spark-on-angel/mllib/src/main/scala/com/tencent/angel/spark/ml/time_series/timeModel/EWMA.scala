package com.tencent.angel.spark.ml.time_series.timeModel

import com.tencent.angel.spark.ml.time_series.stat.TimeSeriesParams
import org.apache.spark.ml.Model
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait EWMAParams extends TimeSeriesParams {
  final val maxEval = new Param[Int](this, "maxEval", "max eval")
  def setMaxEval(value: Int): this.type = set(maxEval, value)

  final val maxIter = new Param[Int](this, "maxIter", "max iteration")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

//  final val initPoint = new Param[Double](this, TimeSeriesConstant.InitPoint, "init point")
//  def setInitPoint(value: Double): this.type = set(initPoint, value)

  final val lambda = new Param[Double](this, "lambda", "lambda")
  def setLambda(value: Double): this.type = set(lambda, value)

}

// class EWMA(override val uid: String) extends Estimator[EWMAModel] with EWMAParams{
//
//  setDefault(timeCol -> "time",
//    timeSeriesCol -> "timeseries")
//
//  def this() = this(Identifiable.randomUID("EWMA"))
//
//  /**
//    * Fits an EWMA model to a time series. Uses the first point in the time series as a starting
//    * value. Uses sum squared error as an objective function to optimize to find smoothing parameter
//    * The model for EWMA is recursively defined as S_t = (1 - a) * X_t + a * S_{t-1}, where
//    * a is the smoothing parameter, X is the original series, and S is the smoothed series
//    * Note that the optimization is performed as unbounded optimization, although in its formal
//    * definition the smoothing parameter is <= 1, which corresponds to an inequality bounded
//    * optimization. Given this, the resulting smoothing parameter should always be sanity checked
//    * https://en.wikipedia.org/wiki/Exponential_smoothing
//    * @param dataset the time series dataset to which we want to fit an EWMA model
//    * @return EWMA model
//    */
//  override def fit(dataset: Dataset[_]): EWMAModel = {
//
//    val data = dataset.select(${timeCol}, ${timeSeriesCol}).rdd.map {
//     // case Row(time: String, value: Double) => (time, value)
//      case Row(time: Double, value: Double) => (time, value)
//      case Row(time: Int, value: Double) => (time.toDouble, value)
//      case Row(time: Float, value: Double) => (time.toDouble, value)
//    }.sortByKey().collect()
//      .map(x => x._2)
//
//    val dataVector = Vectors.dense(data)
//
//    val optimizer = new NonLinearConjugateGradientOptimizer(
//      NonLinearConjugateGradientOptimizer.Formula.FLETCHER_REEVES,
//      new SimpleValueChecker(1e-6, 1e-6))
//
//
//    val gradient = new ObjectiveFunctionGradient(new MultivariateVectorFunction() {
//      def value(params: Array[Double]): Array[Double] = {
//        val g = new EWMAModel(params(0)).gradient(dataVector)
//        Array(g)
//      }
//    })
//
//    val objectiveFunction = new ObjectiveFunction(new MultivariateFunction() {
//      def value(params: Array[Double]): Double = {
//        new EWMAModel(params(0)).sse(dataVector)
//      }
//    })
//    // optimization parameters
//    val initGuess = new InitialGuess(Array(${initPoint}))
//    val goal = GoalType.MINIMIZE
//    // optimization step
//    val optimal = optimizer.optimize(objectiveFunction, goal, gradient, initGuess,
//      new MaxIter(${maxIter}), new MaxEval(${maxEval}))
//    val params = optimal.getPoint
//
//    new EWMAModel(params(0))
//      .setTimeCol(${timeCol})
//      .setTimeSeriesCol(${timeSeriesCol})
//
//  }
//
//  override def copy(extra: ParamMap): Estimator[EWMAModel] = defaultCopy(extra)
//
//  override def transformSchema(schema: StructType): StructType = {
//    schema
//  }
// }


class EWMAModel(override val uid: String, val smoothing: Double)
  extends Model[EWMAModel] with EWMAParams{

  def this(smoothing: Double) = this(Identifiable.randomUID("EWMAModel"), smoothing)
//
//  /**
//    * Calculates the SSE for a given timeseries ts given
//    * the smoothing parameter of the current model
//    * The forecast for the observation at period t + 1 is the smoothed value at time t
//    * Source: http://people.duke.edu/~rnau/411avg.htm
//    * @param ts the time series to fit a EWMA model to
//    * @return Sum Squared Error
//    */
//  def sse(ts: Vector): Double = {
//    val n = ts.size
//
//    val smoothed = addTimeDependentEffects(ts)
//    var i = 0
//    var error = 0.0
//    var sqrErrors = 0.0
//    while (i < n - 1) {
//      error = ts(i + 1) - smoothed(i)
//      sqrErrors += error * error
//      i += 1
//    }
//
//    sqrErrors
//  }
//
//  /**
//    * Calculates the gradient of the SSE cost function for our EWMA model
//    * @return gradient
//    */
//  def gradient(ts: Vector): Double = {
//    val n = ts.size
//    // val smoothed = new DenseVector(Array.fill(n)(0.0))
//    val smoothed = addTimeDependentEffects(ts)
//
//    var error = 0.0
//    var prevSmoothed = ts(0)
//    var prevDSda = 0.0 // derivative of the EWMA function at time t - 1: (d S(t - 1)/ d smoothing)
//    var dSda = 0.0 // derivative of the EWMA function at time t: (d S(t) / d smoothing)
//    var dJda = 0.0 // derivative of our SSE cost function
//    var i = 0
//
//    while (i < n - 1) {
//      error = ts(i + 1) - smoothed(i)
//      dSda = ts(i) - prevSmoothed + (1 - smoothing) * prevDSda
//      dJda += error * dSda
//      prevDSda = dSda
//      prevSmoothed = smoothed(i)
//      i += 1
//    }
//    2 * dJda
//  }

  def addTimeDependentEffects(ts: Vector): Vector = {
    val arr = Array.fill(ts.size)(0.0)
    arr(0) = ts(0) // by definition in our model S_0 = X_0
    for (i <- 1 until ts.size) {
      arr(i) = smoothing * ts(i) + (1 - smoothing) * arr(i - 1)
    }
    new DenseVector(arr)
  }


  override def copy(extra: ParamMap): EWMAModel = defaultCopy(extra)

  /**
    * Transforms the input dataset.
    */
  override def transform(dataset: Dataset[_]): DataFrame = {

    val data = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
      // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()

    val dataVector = Vectors.dense(data.map(x => x._2))

    val results = addTimeDependentEffects(dataVector)

    val res: Array[((Double, Double), Double)] = data.zip(results.toArray)

    val resRDD = dataset.sparkSession.sparkContext.parallelize(
      res.map(x => Row(x._1._1, x._1._2, x._2)))

    val structType = transformSchema(dataset.schema)

//    val resRDD = dataset.sparkSession.sparkContext.parallelize(res.toArray.map(x => Row(x)))
//
//    val structType = transformSchema(dataset.schema)

    dataset.sparkSession.createDataFrame(resRDD, structType)
  }

  /**
    * Check transform validity and derive the output schema from the input schema.
    *
    * Typical implementation should first conduct verification on schema change and parameter
    * validity, including complex parameter interaction checks.
    */
  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(StructField($(timeCol), DoubleType),
      StructField($(timeSeriesCol), DoubleType),
      StructField("prediction", DoubleType)))
  }
}
