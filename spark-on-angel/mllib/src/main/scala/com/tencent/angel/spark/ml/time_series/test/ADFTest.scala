package com.tencent.angel.spark.ml.time_series.test

import breeze.linalg.{DenseMatrix => BreezeDenseMatrix}
import breeze.numerics.polyval
import com.tencent.angel.spark.ml.time_series.stat.TimeSeriesParams
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.{IntParam, Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import com.tencent.angel.spark.ml.util.MatrixUtil
import com.tencent.angel.spark.ml.util.LagUtil
import com.tencent.angel.spark.ml.time_series.transformation.Differentiated

trait ADFTestParams extends TimeSeriesParams with DefaultParamsWritable {
  val maxLag: IntParam = new IntParam(this, "maxLag", "Maximum lag which is included in test")

  def setMaxLag(value: Int): this.type = set(maxLag, value)

  val regression: Param[String] =
    new Param[String](this, "regression",
      "{'c','ct','ctt','nc'} Constant and trend order to include in regression")

  def setRegression(value: String): this.type = set(regression, value)
}

class ADFTest(override val uid: String) extends Transformer with ADFTestParams {

  setDefault(maxLag -> 1, regression -> "c")

  def this() = this(Identifiable.randomUID("ADFTest"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val data = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
      // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()

    val dataVector = Vectors.dense(data.map(x => x._2))

    val result = ADFTest.adfTest(dataVector, $(maxLag), $(regression))

    val spark = dataset.sparkSession
    import spark.implicits._
    val reDF = spark.createDataset(Array(result)).toDF("adfStat", "pValue")
    reDF
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(StructField("adfStat", DoubleType),
      StructField("pValue", DoubleType)))
  }
}

object ADFTest {
  private val ADF_TAU_MIN = Map[String, Array[Double]](
    "nc" -> Array(-19.04, -19.62, -21.21, -23.25, -21.63, -25.74),
    "c" -> Array(-18.83, -18.86, -23.48, -28.07, -25.96, -23.27),
    "ct" -> Array(-16.18, -21.15, -25.37, -26.63, -26.53, -26.18),
    "ctt" -> Array(-17.17, -21.1, -24.33, -24.03, -24.33, -28.22)
  )

  private val ADF_TAU_MAX = Map[String, Array[Double]](
    "nc" -> Array(Double.PositiveInfinity, 1.51, 0.86, 0.88, 1.05, 1.24),
    "c" -> Array(2.74, 0.92, 0.55, 0.61, 0.79, 1),
    "ct" -> Array(0.7, 0.63, 0.71, 0.93, 1.19, 1.42),
    "ctt" -> Array(0.54, 0.79, 1.08, 1.43, 3.49, 1.92)
  )

  private val ADF_TAU_STAR = Map[String, Array[Double]](
    "nc" -> Array(-1.04, -1.53, -2.68, -3.09, -3.07, -3.77),
    "c" -> Array(-1.61, -2.62, -3.13, -3.47, -3.78, -3.93),
    "ct" -> Array(-2.89, -3.19, -3.50, -3.65, -3.80, -4.36),
    "ctt" -> Array(-3.21, -3.51, -3.81, -3.83, -4.12, -4.63)
  )

  private val ADF_TAU_SMALLP = Map[String, Array[Array[Double]]](
    "nc" -> Array(
      Array(0.6344, 1.2378, 3.2496 * 1e-2),
      Array(1.9129, 1.3857, 3.5322 * 1e-2),
      Array(2.7648, 1.4502, 3.4186 * 1e-2),
      Array(3.4336, 1.4835, 3.19 * 1e-2),
      Array(4.0999, 1.5533, 3.59 * 1e-2),
      Array(4.5388, 1.5344, 2.9807 * 1e-2)
    ),
    "c" -> Array(
      Array(2.1659, 1.4412, 3.8269 * 1e-2),
      Array(2.92, 1.5012, 3.9796 * 1e-2),
      Array(3.4699, 1.4856, 3.164 * 1e-2),
      Array(3.9673, 1.4777, 2.6315 * 1e-2),
      Array(4.5509, 1.5338, 2.9545 * 1e-2),
      Array(5.1399, 1.6036, 3.4445 * 1e-2)
    ),
    "ct" -> Array(
      Array(3.2512, 1.6047, 4.9588 * 1e-2),
      Array(3.6646, 1.5419, 3.6448 * 1e-2),
      Array(4.0983, 1.5173, 2.9898 * 1e-2),
      Array(4.5844, 1.5338, 2.8796 * 1e-2),
      Array(5.0722, 1.5634, 2.9472 * 1e-2),
      Array(5.53, 1.5914, 3.0392 * 1e-2)
    ),
    "ctt" -> Array(
      Array(4.0003, 1.658, 4.8288 * 1e-2),
      Array(4.3534, 1.6016, 3.7947 * 1e-2),
      Array(4.7343, 1.5768, 3.2396 * 1e-2),
      Array(5.214, 1.6077, 3.3449 * 1e-2),
      Array(5.6481, 1.6274, 3.3455 * 1e-2),
      Array(5.9296, 1.5929, 2.8223 * 1e-2)
    )
  )

  private val ADF_LARGE_SCALING = Array(1.0, 1e-1, 1e-1, 1e-2)

  private val ADF_TAU_LARGEP = Map[String, Array[Array[Double]]](
    "nc" -> Array(
      Array(0.4797, 9.3557, -0.6999, 3.3066),
      Array(1.5578, 8.558, -2.083, -3.3549),
      Array(2.2268, 6.8093, -3.2362, -5.4448),
      Array(2.7654, 6.4502, -3.0811, -4.4946),
      Array(3.2684, 6.8051, -2.6778, -3.4972),
      Array(3.7268, 7.167, -2.3648, -2.8288)
    ),
    "c" -> Array(
      Array(1.7339, 9.3202, -1.2745, -1.0368),
      Array(2.1945, 6.4695, -2.9198, -4.2377),
      Array(2.5893, 4.5168, -3.6529, -5.0074),
      Array(3.0387, 4.5452, -3.3666, -4.1921),
      Array(3.5049, 5.2098, -2.9158, -3.3468),
      Array(3.9489, 5.8933, -2.5359, -2.721)
    ),
    "ct" -> Array(
      Array(2.5261, 6.1654, -3.7956, -6.0285),
      Array(2.85, 5.272, -3.6622, -5.1695),
      Array(3.221, 5.255, -3.2685, -4.1501),
      Array(3.652, 5.9758, -2.7483, -3.2081),
      Array(4.0712, 6.6428, -2.3464, -2.546),
      Array(4.4735, 7.1757, -2.0681, -2.1196)
    ),
    "ctt" -> Array(
      Array(3.0778, 4.9529, -4.1477, -5.9359),
      Array(3.4713, 5.967, -3.2507, -4.2286),
      Array(3.8637, 6.7852, -2.6286, -3.1381),
      Array(4.2736, 7.6199, -2.1534, -2.4026),
      Array(4.6679, 8.2618, -1.822, -1.9147),
      Array(5.0009, 8.3735, -1.6994, -1.6928)
    )
  ).mapValues {
    arr =>
      arr.map {
        subarr => (0 until 4).map(i => ADF_LARGE_SCALING(i) * subarr(i)).toArray
      }
  }

  private def vanderFlipped(vec: Array[Double], n: Int): Matrix = {
    val numRows = vec.length
    val matArr = Array.fill[Double](numRows * n)(1.0)
    val mat = new DenseMatrix(numRows, n, matArr)
    val breezeMat = MatrixUtil.toBreeze(mat)

    for (c <- 1 until n) {
      for (r <- 0 until numRows) {
        breezeMat(r, c) = vec(r) * mat(r, c - 1)
      }
    }
    mat
  }

  private def addTrend(mat: Matrix, trend: String = "c", prepend: Boolean = false): Matrix = {
    val trendOrder = trend.toLowerCase match {
      case "c" => 0
      case "ct" | "t" => 1
      case "ctt" => 2
      case _ => throw new IllegalArgumentException(s"Trend $trend is not c, ct, or ctt")
    }

    val nObs = mat.numRows
    var trendMat = MatrixUtil.toBreeze(
      vanderFlipped((1 until nObs + 1).map(_.toDouble).toArray, trendOrder + 1))

    if (trend == "t") {
      trendMat = trendMat(0 to trendMat.rows, 1 to 2)
    }

    MatrixUtil.fromBreeze(if (prepend) {
      BreezeDenseMatrix.horzcat(trendMat, MatrixUtil.toBreeze(mat))
    } else {
      BreezeDenseMatrix.horzcat(MatrixUtil.toBreeze(mat), trendMat)
    })
  }

  /**
   * Returns MacKinnon's approximate p-value for testStat.
   *
   * @param testStat   "T-value" from an Augmented Dickey-Fuller regression.
   * @param regression This is the method of regression that was used.  Following MacKinnon's
   *                   notation, this can be "c" for constant, "nc" for no constant, "ct" for
   *                   constant and trend, and "ctt" for constant, trend, and trend-squared.
   * @param n          The number of series believed to be I(1).  For (Augmented) Dickey-
   *                   Fuller N = 1.
   * @return The p-value for the ADF statistic estimated using MacKinnon 1994.
   */
  private def mackinnonp(testStat: Double, regression: String = "c", n: Int = 1): Double = {
    val maxStat = ADF_TAU_MAX(regression)
    if (testStat > maxStat(n - 1)) {
      return 1.0
    }
    val minStat = ADF_TAU_MIN(regression)
    if (testStat < minStat(n - 1)) {
      return 0.0
    }
    val starStat = ADF_TAU_STAR(regression)
    val tauCoef = if (testStat <= starStat(n - 1)) {
      ADF_TAU_SMALLP(regression)(n - 1)
    } else {
      ADF_TAU_LARGEP(regression)(n - 1)
    }
    new NormalDistribution().cumulativeProbability(polyval(tauCoef, testStat))
  }

  /**
   * Augmented Dickey-Fuller test for a unit root in a univariate time series.
   * Ng and Perron(2001), Lag length selection and the construction of unit root
   * tests with good size and power, Econometrica, Vol 69 (6) pp 1519-1554
   *
   * The null hypothesis is that the time series contains a unit root, implying that
   * differencing is required to make it stationary.  The alternative hypothesis is that the
   * time series is stationary.  Lower values of the test statistic imply lower p-values,
   * and thus higher likelihood of rejecting the null hypothesis.
   *
   * @param ts         The time series.
   * @param maxLag     Maximum lag which is included in test, default 12*(nobs/100)^{1/4}
   * @param regression {'c','ct','ctt','nc'} Constant and trend order to include in regression
   *                   'c' : constant only (default)
   *                   'ct' : constant and trend
   *                   'ctt' : constant, and linear and quadratic trend
   *                   'nc' : no constant, no trend
   * @return A tuple containing the test statistic and p value.
   */
  def adfTest(ts: Vector, maxLag: Int, regression: String = "c"): (Double, Double) = {
    val breezeTs = MatrixUtil.toBreeze(ts)
    val tsDiff =
      new DenseVector(Differentiated.differencesAtLag(ts, 1).toArray.slice(1, ts.size))
    val lagMat = LagUtil.lagMatTrimBoth(tsDiff, maxLag, includeOriginal = true)
    val nObs = lagMat.numRows

    MatrixUtil.toBreeze(lagMat)(0 until nObs, 0 to 0) :=
      breezeTs(ts.size - nObs - 1 until ts.size - 1).toDenseVector.toDenseMatrix.t

    val tsdShort = MatrixUtil.toBreeze(tsDiff)(tsDiff.size - nObs until tsDiff.size)
    val ols = new OLSMultipleLinearRegression()
    ols.setNoIntercept(true)
    if (regression != "nc") {
      val withTrend = MatrixUtil.matToRowArrs(addTrend(lagMat, regression))
      ols.newSampleData(tsdShort.toArray, withTrend)
    } else {
      ols.newSampleData(tsdShort.toArray, MatrixUtil.matToRowArrs(lagMat))
    }

    val olsParamStandardErrors = ols.estimateRegressionParametersStandardErrors()
    val coefficients = ols.estimateRegressionParameters()
    val adfStat = coefficients(0) / olsParamStandardErrors(0)

    val pValue = mackinnonp(adfStat, regression)
    (adfStat, pValue)
  }
}
