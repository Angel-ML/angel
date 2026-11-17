package com.tencent.angel.spark.ml.time_series.timeModel

import com.tencent.angel.spark.ml.time_series.stat.TimeSeriesParams
import com.tencent.angel.spark.ml.time_series.test.KpssTest
import com.tencent.angel.spark.ml.time_series.transformation.Differentiated
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.HashSet
import scala.util.Try

trait AutoARIMAParams extends TimeSeriesParams{

  final val maxEval = new Param[Int](this, "maxEval", "max eval")
  def setMaxEval(value: Int): this.type = set(maxEval, value)

  final val maxIter = new Param[Int](this, "maxIter", "max iteration")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  final val maxD = new Param[Int](this, "maxD", "limit for differencing order")
  def setMaxD(value: Int): this.type = set(maxD, value)

  final val maxP = new Param[Int](this, "maxP", "limit for the AR order")
  def setMaxP(value: Int): this.type = set(maxP, value)

  final val maxQ = new Param[Int](this, "maxQ", "limit for the MA order")
  def setMaxQ(value: Int): this.type = set(maxQ, value)

  final val includeIntercept = new Param[Boolean](this, "includeIntercept", "Add intercept term")
  def setIncludeIntercept(value: Boolean): this.type = set(includeIntercept, value)
}

class AutoARIMA(override val uid: String) extends Estimator[ARIMAModel] with AutoARIMAParams {
  def this() = this(Identifiable.randomUID("AutoARIMA"))

  override def fit(dataset: Dataset[_]): ARIMAModel = {
    val data = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
      // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()

    val timeVector = data.map(x => x._1)
    val ts = Vectors.dense(data.map(x => x._2))
    // p-value threshold for stationarity test
    val kpssSignificance = 0.05

    // Our first task is to choose a differencing order.
    // Following R's forecast::auto.arima -> forecast::ndiffs -> tseries::kpss.test, we test for
    // level stationarity and return lowest differencing order that results in level stationarity.
    val dOpt = (0 to $(maxD)).find { diff =>
      val testTs = Differentiated.differencesOfOrderD(ts, diff)
      val (stat, criticalValues) = KpssTest.kpsstest(testTs, "c")
      stat < criticalValues(kpssSignificance)
    }
    val d = dOpt match {
      case Some(v) => v
      case None =>
        // this gets wrapped in Try at top level, so not necessarily bad to throw exception
        throw new Exception(s"stationarity not achieved with differencing order <= $maxD")
    }
    val diffedTs = Differentiated.differencesOfOrderD(ts, d)
    // As per heuristic, only fit intercept if differencing is <= 1 for non-seasonal arima
    val addIntercept = d <= 1

    val spark = dataset.sparkSession

    val bestModel = findBestARMAModel(spark, timeVector, diffedTs, $(maxP), $(maxQ), addIntercept)
    new ARIMAModel(bestModel.p, d, bestModel.q, bestModel.coefficients, bestModel.hasIntercept)
  }

  /**
    * As described by the "A step-wise procedure for traversing the model space" section Hyndman
    * and Kandahar.
    */
  private def findBestARMAModel(spark: SparkSession,
                                timeVector: Array[Double],
                                diffedTs: Vector,
                                maxP: Int,
                                maxQ: Int,
                                startWithIntercept: Boolean): ARIMAModel = {
    def fitTryBothStrategies(p: Int, q: Int, timeVector: Array[Double],
                             ts: Vector, intercept: Boolean): Try[ARIMAModel] = {
      val result = timeVector.zip(diffedTs.toArray)
      import spark.implicits._

      val dataFrame = spark.createDataset(result).toDF($(timeCol), $(timeSeriesCol))

      Try{
        val arima = new ARIMA()
          .setP(p)
          .setQ(q)
          .setD(0)
          .setIncludeIntercept(intercept)
          .setMethod("css-cgd")
          .setTimeCol($(timeCol))
          .setTimeSeriesCol($(timeSeriesCol))
        arima.fit(dataFrame)

      }.orElse {
        Try{
          val arima = new ARIMA()
            .setP(p)
            .setQ(q)
            .setD(0)
            .setIncludeIntercept(intercept)
            .setMethod("css-bobyqa")
            .setTimeCol($(timeCol))
            .setTimeSeriesCol($(timeSeriesCol))
          arima.fit(dataFrame)
        }
      }
    }

    // Maintain a set of parameters we've tried so we don't waste time repeating
    val pastParams = new HashSet[(Int, Int, Boolean)]()
    var curBestAIC = Double.MaxValue
    var curBestModel: ARIMAModel = null
    var done = false

    var nextParams = List((0, 0), (2, 2), (1, 0), (0, 1)).map { case (p, q) =>
      (p, q, startWithIntercept)
    }

    // Conduct a local search for a best model by varying p and q around the current best. We stop
    // when no models with nearby p's and q's exceed the AIC of the current best.
    while (!done) {
      pastParams ++= nextParams
      // Fit models to next params.
      val models = nextParams.map { case (p, q, intercept) =>
        fitTryBothStrategies(p, q, timeVector, diffedTs, intercept)
      }.filter(_.isSuccess).map(_.get)

      // Compute AICs and filter out models with worse (larger) AICs than our current best.
      // Also only retain models that are stationary and invertible.
      val modelsAndAICs = models.filter(m => m.isStationary && m.isInvertible).map(
        model => (model, model.approxAIC(diffedTs)))
      val improvingModelsAndAICs = modelsAndAICs.filter(_._2 < curBestAIC)

      if (improvingModelsAndAICs.isEmpty) {
        done = true
      } else {
        // Pick params for next iteration based on new incumbent model
        val newBest = improvingModelsAndAICs.minBy(_._2)
        curBestModel = newBest._1
        curBestAIC = newBest._2

        // Try variations of +-1 p and q around the current model, along with keeping p and q the
        // same, but flipping whether to include an intercept.
        val deltas = List(-1, 0, 1)
        // Each param tuple is a triple of (p, q, includeIntercept)
        val surroundingParams = for { pDelta <- deltas; qDelta <- deltas } yield {
          val includeIntercept = if (pDelta == 0 && qDelta == 0) {
            !curBestModel.hasIntercept
          } else {
            curBestModel.hasIntercept
          }
          (curBestModel.p + pDelta, curBestModel.q, includeIntercept)
        }

        // Filter out params we've already tried, as well as params that are outside our p and q
        // bounds.
        nextParams = surroundingParams.filter(params => !pastParams.contains(params) &&
          params._1 >= 0 && params._1 <= maxP && params._2 >= 0 && params._2 <= maxQ)
      }
    }

    curBestModel
  }

  override def copy(extra: ParamMap): Estimator[ARIMAModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema
  }
}
