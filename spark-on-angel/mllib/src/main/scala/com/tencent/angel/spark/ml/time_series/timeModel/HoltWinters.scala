package com.tencent.angel.spark.ml.time_series.timeModel

import com.tencent.angel.spark.ml.time_series.stat.TimeSeriesParams
import org.apache.commons.math3.analysis.MultivariateFunction
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.BOBYQAOptimizer
import org.apache.commons.math3.optim.nonlinear.scalar.{GoalType, ObjectiveFunction}
import org.apache.commons.math3.optim.{InitialGuess, MaxEval, MaxIter, SimpleBounds}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait HoltWintersParams extends TimeSeriesParams {
  final val maxEval = new Param[Int](this, "maxEval", "max eval")

  def setMaxEval(value: Int): this.type = set(maxEval, value)

  final val maxIter = new Param[Int](this, "maxIter", "max iteration")

  def setMaxIter(value: Int): this.type = set(maxIter, value)

  final val period = new Param[Int](this, "period", "Seasonality of data")

  def setPeriod(value: Int): this.type = set(period, value)

  final val modelType = new Param[String](this, "modelType", "Two variations " +
    "differ in the nature of the seasonal component. Additive method is preferred when seasonal " +
    "variations are roughly constant through the series, Multiplicative method is preferred when " +
    "the seasonal variations are changing proportional to the level of the series")

  def setModelType(value: String): this.type = set(modelType, value)
}

/**
 * Triple exponential smoothing takes into account seasonal changes as well as trends.
 * Seasonality is deﬁned to be the tendency of time-series data to exhibit behavior that repeats
 * itself every L periods, much like any harmonic function.
 *
 * The Holt-Winters method is a popular and effective approach to forecasting seasonal time series
 *
 * See https://en.wikipedia.org/wiki/Exponential_smoothing#Triple_exponential_smoothing
 * for more information on Triple Exponential Smoothing
 * See https://www.otexts.org/fpp/7/5 and
 * https://stat.ethz.ch/R-manual/R-devel/library/stats/html/HoltWinters.html
 * for more information on Holt Winter Method.
 */
class HoltWinters(override val uid: String) extends Estimator[HoltWintersModel] with HoltWintersParams {

  setDefault(timeCol -> "time",
    timeSeriesCol -> "timeseries",
    maxEval -> 10000,
    maxIter -> 10000)

  def this() = this(Identifiable.randomUID("HoltWinters"))

  /**
   * Fits a model to the input data.
   */
  override def fit(dataset: Dataset[_]): HoltWintersModel = {

    val data = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
      // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()

    val dataVector = Vectors.dense(data.map(x => x._2))
    val optimizer = new BOBYQAOptimizer(7)

    val objectiveFunction = new ObjectiveFunction(new MultivariateFunction() {
      def value(params: Array[Double]): Double = {
        new HoltWintersModel(params(0), params(1), params(2))
          .setModelType($(modelType))
          .setPeriod($(period))
          .sse(dataVector)
      }
    })

    // The starting guesses in R's stats:HoltWinters
    val initGuess = new InitialGuess(Array(0.3, 0.1, 0.1))
    val goal = GoalType.MINIMIZE
    val bounds = new SimpleBounds(Array(0.0, 0.0, 0.0), Array(1.0, 1.0, 1.0))
    val optimal = optimizer.optimize(objectiveFunction, goal, bounds, initGuess, new MaxIter($(maxIter)), new MaxEval($(maxEval)))
    val params = optimal.getPoint
    new HoltWintersModel(params(0), params(1), params(2))
      .setModelType($(modelType))
      .setPeriod($(period))
      .setTimeCol($(timeCol))
      .setTimeSeriesCol($(timeSeriesCol))
  }

  override def copy(extra: ParamMap): Estimator[HoltWintersModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema
}

class HoltWintersModel(override val uid: String, val alpha: Double, val beta: Double, val gamma: Double)
  extends Model[HoltWintersModel] with HoltWintersParams {

  def this(alpha: Double, beta: Double, gamma: Double) = this(Identifiable.randomUID
  ("HoltWintersModel"), alpha, beta, gamma)

  override def copy(extra: ParamMap): HoltWintersModel = defaultCopy(extra)

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

    val destArr = new Array[Double]($(period))
    val (fitted, level, trend, season) = getHoltWintersComponents(dataVector)
    val n = dataVector.size

    val finalLevel = level(n - $(period))
    val finalTrend = trend(n - $(period))
    val finalSeason = new Array[Double]($(period))

     for (i <- 0 until $(period)) {
      finalSeason(i) = season(i + n - $(period))
    }

    for (i <- 0 until $(period)) {
      destArr(i) = if ($(modelType).equalsIgnoreCase("additive")) {
        (finalLevel + (i + 1) * finalTrend) + finalSeason(i % $(period))
      } else {
        (finalLevel + (i + 1) * finalTrend) * finalSeason(i % $(period))
      }
    }

    val dummyArr = Array.fill[(Double, Double)]($(period))((0.0, 0.0))
    val fillObserved = data ++ dummyArr

    val fillFitted = fitted.toArray ++ destArr
    val res = fillObserved.zip(fillFitted)


    val resRDD = dataset.sparkSession.sparkContext.parallelize(
      res.map(x => Row(x._1._1, x._1._2, x._2)))

    val structType = transformSchema(dataset.schema)

    dataset.sparkSession.createDataFrame(resRDD, structType)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(StructField($(timeCol), DoubleType),
      StructField($(timeSeriesCol), DoubleType),
      StructField("prediction", DoubleType)))
  }

  /**
   * Calculates sum of squared errors, used to estimate the alpha and beta parameters
   *
   * @param ts A time series for which we want to calculate the SSE, given the current parameters
   * @return SSE
   */
  def sse(ts: Vector): Double = {
    val n = ts.size
    val smoothed = addTimeDependentEffects(ts)

    var error = 0.0
    var sqrErrors = 0.0

    // We predict only from period by using the first period - 1 elements.
    for (i <- $(period) until n) {
      error = ts(i) - smoothed(i)
      sqrErrors += error * error
    }

    sqrErrors
  }

  def addTimeDependentEffects(ts: Vector): Vector = {
    val destArr = Array.fill(ts.size)(0.0)
    val fitted = getHoltWintersComponents(ts)._1
    for (i <- 0 until ts.size) {
      destArr(i) = fitted(i)
    }
    Vectors.dense(destArr)
  }

  /**
   * Start from the intial parameters and then iterate to find the final parameters
   * using the equations of HoltWinter Method.
   * See https://www.otexts.org/fpp/7/5 and
   * https://stat.ethz.ch/R-manual/R-devel/library/stats/html/HoltWinters.html
   * for more information on Holt Winter Method equations.
   *
   * @param ts A time series for which we want the HoltWinter parameters level,trend and season.
   * @return (level trend season). Final vectors of level trend and season are returned.
   */
  def getHoltWintersComponents(ts: Vector): (Vector, Vector, Vector, Vector) = {
    val n = ts.size
    require(n >= 2, "Requires length of at least 2")

    val dest = new Array[Double](n)

    val level = new Array[Double](n)
    val trend = new Array[Double](n)
    val season = new Array[Double](n)

    val (initLevel, initTrend, initSeason) = initHoltWinters(ts)
    level(0) = initLevel
    trend(0) = initTrend
    for (i <- initSeason.indices) {
      season(i) = initSeason(i)
    }

    for (i <- 0 until (n - $(period))) {
      dest(i + $(period)) = level(i) + trend(i)

      // Add the seasonal factor for additive and multiply for multiplicative model.
      if ($(modelType).equalsIgnoreCase("additive")) {
        dest(i + $(period)) += season(i)
      } else {
        dest(i + $(period)) *= season(i)
      }

      val levelWeight = if ($(modelType).equalsIgnoreCase("additive")) {
        ts(i + $(period)) - season(i)
      } else {
        ts(i + $(period)) / season(i)
      }

      level(i + 1) = alpha * levelWeight + (1 - alpha) * (level(i) + trend(i))

      trend(i + 1) = beta * (level(i + 1) - level(i)) + (1 - beta) * trend(i)

      val seasonWeight = if ($(modelType).equalsIgnoreCase("additive")) {
        ts(i + $(period)) - level(i + 1)
      } else {
        ts(i + $(period)) / level(i + 1)
      }
      season(i + $(period)) = gamma * seasonWeight + (1 - gamma) * season(i)
    }

    (Vectors.dense(dest), Vectors.dense(level), Vectors.dense(trend), Vectors.dense(season))
  }

  def getKernel: (Array[Double]) = {
    if ($(period) % 2 == 0) {
      val kernel = Array.fill($(period) + 1)(1.0 / $(period))
      kernel(0) = 0.5 / $(period)
      kernel($(period)) = 0.5 / $(period)
      kernel
    } else {
      Array.fill($(period))(1.0 / $(period))
    }
  }

  /**
   * Function to calculate the Weighted moving average/convolution using above kernel/weights
   * for input data.
   * See http://robjhyndman.com/papers/movingaverage.pdf for more information
   *
   * @param inData Series on which you want to do moving average
   * @param kernel Weight vector for weighted moving average
   */
  def convolve(inData: Array[Double], kernel: Array[Double]): (Array[Double]) = {
    val kernelSize = kernel.length
    val dataSize = inData.length

    val outData = new Array[Double](dataSize - kernelSize + 1)

    var end = 0
    while (end <= (dataSize - kernelSize)) {
      var sum = 0.0
      for (i <- 0 until kernelSize) {
        sum += kernel(i) * inData(end + i)
      }
      outData(end) = sum
      end += 1
    }
    outData
  }

  /**
   * Function to get the initial level, trend and season using method suggested in
   * http://robjhyndman.com/hyndsight/hw-initialization/
   *
   * @param ts vector
   */
  def initHoltWinters(ts: Vector): (Double, Double, Array[Double]) = {
    val arrTs = ts.toArray

    // Decompose a window of time series into level trend and seasonal using convolution
    val kernel = getKernel
    val kernelSize = kernel.length
    val trend = convolve(arrTs.take($(period) * 2), kernel)

    // Remove the trend from time series. Subtract for additive and divide for multiplicative
    val n = (kernelSize - 1) / 2
    val removeTrend = arrTs.take($(period) * 2).zip(
      Array.fill(n)(0.0) ++ trend ++ Array.fill(n)(0.0)).map {
      case (a, t) =>
        if (t != 0) {
          if ($(modelType).equalsIgnoreCase("additive")) {
            a - t
          } else {
            a / t
          }
        } else {
          0
        }
    }

    // seasonal mean is sum of mean of all season values of that period
    val seasonalMean = removeTrend.splitAt($(period)).zipped.map { case (prevx, x) =>
      if (prevx == 0 || x == 0) x + prevx else (x + prevx) / 2
    }

    val meanOfFigures = seasonalMean.sum / $(period)

    // The seasonal mean is then centered and removed to get season.
    // Subtract for additive and divide for multiplicative.
    val initSeason = if ($(modelType).equalsIgnoreCase("additive")) {
      seasonalMean.map(_ - meanOfFigures)
    } else {
      seasonalMean.map(_ / meanOfFigures)
    }

    // Do Simple Linear Regression to find the initial level and trend
    val indices = 1 to trend.length
    val xbar = (indices.sum: Double) / indices.size
    val ybar = trend.sum / trend.length

    val xxbar = indices.map(x => (x - xbar) * (x - xbar)).sum
    val xybar = indices.zip(trend).map {
      case (x, y) => (x - xbar) * (y - ybar)
    }.sum

    val initTrend = xybar / xxbar
    val initLevel = ybar - (initTrend * xbar)

    (initLevel, initTrend, initSeason)
  }
}

