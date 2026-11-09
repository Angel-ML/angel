package com.tencent.angel.spark.ml.time_series.timeModel

import com.tencent.angel.spark.ml.time_series.stat.TimeSeriesParams
import org.apache.commons.math3.analysis.{MultivariateFunction, MultivariateVectorFunction}
import org.apache.commons.math3.optim.nonlinear.scalar.gradient.NonLinearConjugateGradientOptimizer
import org.apache.commons.math3.optim.nonlinear.scalar.{ObjectiveFunction, ObjectiveFunctionGradient}
import org.apache.commons.math3.optim.{InitialGuess, MaxEval, MaxIter, SimpleValueChecker}
import org.apache.commons.math3.random.RandomGenerator
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait GARCHParams extends TimeSeriesParams {
  final val maxEval = new Param[Int](this, "maxEval", "max eval")
  def setMaxEval(value: Int): this.type = set(maxEval, value)

  final val maxIter = new Param[Int](this, "maxIter", "max iteration")
  def setMaxIter(value: Int): this.type = set(maxIter, value)
}

class GARCH(override val uid: String) extends Estimator[GARCHModel] with GARCHParams{
  setDefault(timeCol -> "time",
    timeSeriesCol -> "timeseries",
    maxEval -> 10000,
    maxIter -> 10000)

  def this() = this(Identifiable.randomUID("GARCH"))

  /**
   * Fits a GARCH(1, 1) model to the given time series.
   */
  override def fit(dataset: Dataset[_]): GARCHModel = {

    val data = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
      // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()

    val dataVector = Vectors.dense(data.map(x => x._2))

    val optimizer = new NonLinearConjugateGradientOptimizer(
      NonLinearConjugateGradientOptimizer.Formula.FLETCHER_REEVES,
      new SimpleValueChecker(1e-6, 1e-6))

    val gradient = new ObjectiveFunctionGradient(new MultivariateVectorFunction() {
      def value(params: Array[Double]): Array[Double] = {
        new GARCHModel(params(0), params(1), params(2)).gradient(dataVector)
      }
    })
    val objectiveFunction = new ObjectiveFunction(new MultivariateFunction() {
      def value(params: Array[Double]): Double = {
        new GARCHModel(params(0), params(1), params(2)).logLikelihood(dataVector)
      }
    })

    val initialGuess = new InitialGuess(Array(.2, .2, .2))

    val optimal = optimizer.optimize(objectiveFunction, gradient, initialGuess,
      new MaxIter($(maxIter)), new MaxEval($(maxEval)))

    val params = optimal.getPoint
    new GARCHModel(params(0), params(1), params(2))
      .setTimeCol($(timeCol)).setTimeSeriesCol($(timeSeriesCol))

  }

  override def copy(extra: ParamMap): Estimator[GARCHModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema
}

class GARCHModel(override val uid: String, val omega: Double, val alpha: Double, val beta: Double)
  extends Model[GARCHModel] with GARCHParams {

  def this(omega: Double, alpha: Double, beta: Double) = this(Identifiable.randomUID("GARCH"),
    omega, alpha, beta)

  override def copy(extra: ParamMap): GARCHModel = defaultCopy(extra)

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

    dataset.sparkSession.createDataFrame(resRDD, structType)
  }

  override def transformSchema(schema: StructType): StructType =
    StructType(Array(StructField($(timeCol), DoubleType),
      StructField($(timeSeriesCol), DoubleType),
      StructField("prediction", DoubleType)))

  /**
   * Returns the log likelihood of the parameters on the given time series.
   *
   * Based on https://pdfs.semanticscholar.org/7da8/bfa5295375c1141d797e80065a599153c19d.pdf
   */
  def logLikelihood(ts: Vector): Double = {
    var sum = 0.0
    iterateWithHAndEta(ts) { (i, h, eta, prevH, prevEta) =>
      sum += -.5 * math.log(h) - .5 * eta * eta / h
    }
    sum + -.5 * math.log(2 * math.Pi) * (ts.size - 1)
  }

  private def iterateWithHAndEta(ts: Vector)
                                (fn: (Int, Double, Double, Double, Double) => Unit): Unit = {
    var prevH = omega / (1 - alpha - beta)
    var i = 1
    while (i < ts.size) {
      val h = omega + alpha * ts(i - 1) * ts(i - 1) + beta * prevH
      fn(i, h, ts(i), prevH, ts(i - 1))
      prevH = h
      i += 1
    }
  }

  def gradient(ts: Vector): Array[Double] = {
    var omegaGradient = 0.0
    var alphaGradient = 0.0
    var betaGradient = 0.0
    var omegaDhdtheta = 0.0
    var alphaDhdtheta = 0.0
    var betaDhdtheta = 0.0
    iterateWithHAndEta(ts) { (i, h, eta, prevH, prevEta) =>
      omegaDhdtheta = 1 + beta * omegaDhdtheta
      alphaDhdtheta = prevEta * prevEta + beta * alphaDhdtheta
      betaDhdtheta = prevH + beta * betaDhdtheta

      val multiplier = (eta * eta / (h * h)) - (1 / h)
      omegaGradient += multiplier * omegaDhdtheta
      alphaGradient += multiplier * alphaDhdtheta
      betaGradient += multiplier * betaDhdtheta
    }
    Array(alphaGradient * .5, betaGradient * .5, omegaGradient * .5)
  }

  def addTimeDependentEffects(ts: Vector): Vector = {

    val destArr = new Array[Double](ts.size)

    var prevVariance = omega / (1.0 - alpha - beta)
    var prevEta = ts(0) * math.sqrt(prevVariance)

    destArr(0) = prevEta
    for (i <- 1 until ts.size) {
      val variance = omega + alpha * prevEta * prevEta + beta * prevVariance
      val standardizedEta = ts(i)
      val eta = standardizedEta * math.sqrt(variance)
      destArr(i) = eta

      prevEta = eta
      prevVariance = variance
    }
    new DenseVector(destArr)
  }

  private def sampleWithVariances(n: Int, rand: RandomGenerator): (Array[Double], Array[Double]) = {
    val ts = new Array[Double](n)
    val variances = new Array[Double](n)
    variances(0) = omega / (1 - alpha - beta)
    var eta = math.sqrt(variances(0)) * rand.nextGaussian()
    for (i <- 1 until n) {
      variances(i) = omega + beta * variances(i-1) + alpha * eta * eta
      eta = math.sqrt(variances(i)) * rand.nextGaussian()
      ts(i) = eta
    }

    (ts, variances)
  }

  /**
   * Samples a random time series of a given length with the properties of the model.
   *
   * @param n The length of the time series to sample.
   * @param rand The random generator used to generate the observations.
   * @return The samples time series.
   */
  def sample(n: Int, rand: RandomGenerator): Array[Double] = sampleWithVariances(n, rand)._1
}