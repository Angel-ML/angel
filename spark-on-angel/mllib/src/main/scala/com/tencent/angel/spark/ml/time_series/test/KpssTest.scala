package com.tencent.angel.spark.ml.time_series.test

import com.tencent.angel.spark.ml.time_series.stat.TimeSeriesParams
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}


trait KpssTestParams extends TimeSeriesParams with DefaultParamsWritable {
  val method: Param[String] =
    new Param[String](this, "nobs", "Maximum lag which is included in test")

  def setMethod(value: String): this.type = set(method, value)
}

class KpssTest(override val uid: String) extends Transformer with KpssTestParams {

  setDefault(method -> "c")

  def this() = this(Identifiable.randomUID("KpssTest"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val data = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
      // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()

    val dataVector = Vectors.dense(data.map(x => x._2))

    val (stat, criticalValues) = KpssTest.kpsstest(dataVector, $(method))
    val spark = dataset.sparkSession

    val result = criticalValues.map { x =>
      (stat, x._1, x._2)
    }.toArray

    import spark.implicits._
    val reDF = spark.createDataset(result).toDF("stat", "criticalLevel", "criticalValue")
    reDF
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(StructField("stat", DoubleType),
      StructField("criticalLevel", DoubleType),
      StructField("criticalValue", DoubleType)
    ))
  }
}

object KpssTest {
  /**
   * Critical values associated with KPSS test when testing with a simple constant
   * Source:
   * Kwiatkowski, D., Phillips, P., Schmidt, P., & Shin, Y.
   * Testing the null hypothesis of stationarity against the alternative of a unit root.
   * Journal of Econometrics, 159-178.
   */
  private val kpssConstantCriticalValues = Map(
    0.10 -> 0.347, 0.05 -> 0.463, 0.025 -> 0.574, 0.01 -> 0.739
  )

  /**
   * Critical values associated with KPSS test when testing with a constant and time trend
   * Source:
   * Kwiatkowski, D., Phillips, P., Schmidt, P., & Shin, Y.
   * Testing the null hypothesis of stationarity against the alternative of a unit root.
   * Journal of Econometrics, 159-178.
   */
  private val kpssConstantAndTrendCriticalValues = Map(
    0.10 -> 0.119, 0.05 -> 0.146, 0.025 -> 0.176, 0.01 -> 0.216
  )

  /**
   * Performs the KPSS stationarity test (Kwiatkowski, Phillips, Schmidt and Shin). The null
   * hypothesis corresponds to stationarity (level stationarity if method = "c", or trend
   * stationarity if method = "ct"). If method = "c", a regression of the form ts_i = alpha +
   * error_i is fit. If method = "ct", a regression of the form ts_i = alpha + beta * i + error_i.
   * The test then performs a check for the variance of the errors. The null hypothesis of
   * stationarity corresponds to a null hypothesis of variance = 0 for the errors.
   * For more information please see pg 129 of [[http://faculty.washington
   * .edu/ezivot/econ584/notes/unitroot.pdf]]. For the original paper on the test, please see
   * [[http://www.deu.edu.tr/userweb/onder.hanedar/dosyalar/kpss.pdf]].
   * Finally, the current implementation follows R's tseries package implementation closely, which
   * can be found at [[https://cran.r-project.org/web/packages/tseries/index.html]]
   *
   * @param ts     time series to test for stationarity
   * @param method "c" or "ct", short for fitting with a constant or a constant and a time trend
   * @return the KPSS statistic and a map of critical values according to the method selected
   */
  def kpsstest(ts: Vector, method: String): (Double, Map[Double, Double]) = {
    require(List("c", "ct").contains(method), "trend must be c or ct")
    val n = ts.size
    // constant
    val c = Array.fill(n)(1.0)
    // create appropriate regression matrix and obtain critical values
    val (regressors, criticalValues) = if (method == "c") {
      (c.map(Array(_)), kpssConstantCriticalValues)
    } else {
      // time trend
      val t = Array.tabulate(n)(x => 1.0 + x.toDouble)
      (Array(c, t).transpose, kpssConstantAndTrendCriticalValues)
    }
    val ols = new OLSMultipleLinearRegression()
    ols.setNoIntercept(true)
    ols.newSampleData(ts.toArray, regressors)
    val residuals = ols.estimateResiduals()
    // sum of (square of cumulative sum of errors)
    val s2 = residuals.scanLeft(0.0)(_ + _).tail.map(math.pow(_, 2)).sum
    // long run variance estimate using Newey-West estimator
    // we follow the default lag used in kpss.test in R's tseries package
    val lag = (3 * math.sqrt(n) / 13).toInt
    val longRunVariance = neweyWestVarianceEstimator(residuals, lag)
    val stat = (s2 / longRunVariance) / (n * n)
    (stat, criticalValues)
  }

  /**
   * Estimates long run variance using the Newey-West estimator, which consists on calculating
   * the variance between lagged values and applying a weight that decreases as the lag increases.
   * We translate the C implementation used by R's tseries:kpss.test, found at
   * https://github.com/cran/tseries/blob/master/src/ppsum.c
   * However, we add our own comments for clarity.
   * See pg 87 of [[http://faculty.washington.edu/ezivot/econ584/notes/timeSeriesConcepts.pdf]] for
   * more information
   */
  private def neweyWestVarianceEstimator(errors: Array[Double], lag: Int): Double = {
    val n = errors.length
    var sumOfTerms = 0.0
    var cellContrib = 0.0
    var i = 0
    var j = 0

    i = 1
    while (i <= lag) {
      j = i
      cellContrib = 0.0
      while (j < n) {
        // covariance between values at time t and time t-i
        cellContrib += errors(j) * errors(j - i)
        j += 1
      }
      // Newey-West weighing (decreasing weight for longer lags)
      sumOfTerms += cellContrib * (1 - (i.toDouble / (lag + 1)))
      i += 1
    }
    // we multiply by 2 as we calculated the sum of top row in matrix (lag 0, lag 1), (lag 0, lag 2)
    // etc but we need to also calculate first column (lag 1, lag 0), (lag 2, lag 0) etc
    // we divide by n and obtain a partial estimate of the variance, just missing no lag variance
    val partialEstVar = (sumOfTerms * 2) / n
    // add no-lag variance
    partialEstVar + (errors.map(math.pow(_, 2)).sum / n)
  }
}
