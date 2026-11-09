package com.tencent.angel.spark.ml.time_series.transformation


import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{BooleanParam, IntParam, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.StructType
import com.tencent.angel.spark.ml.time_series.stat.TimeSeriesParams

trait DifferentiatedParams extends TimeSeriesParams with DefaultParamsWritable {
  val d: IntParam = new IntParam(this, "d", "differencing order")
  def getD: Double = $(d)

  val inverse: BooleanParam = new BooleanParam(this, "inverse", "whether inverse differentiated")
  def getInverse: Boolean = $(inverse)
}


class Differentiated(override val uid: String) extends Transformer with DifferentiatedParams{
  def this() = this(Identifiable.randomUID("Differentiated"))

  def setD(value: Int): this.type = set(d, value)

  def setInverse(value: Boolean): this.type = set(inverse, value)

  setDefault(inverse -> false, d -> 1)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val data = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
     // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()


    val timeVector = data.map(x => x._1)
    val dataVector = Vectors.dense(data.map(x => x._2))

    // Drop first d terms, can't use those to fit, since not at the right level of differencing
    val diffedTs = if (!$(inverse)) {
      Differentiated.differencesOfOrderD(dataVector, $(d)).toArray // .drop(${d})
    }
    else {
      Differentiated.inverseDifferencesOfOrderD(dataVector, $(d)).toArray  // .drop(${d})
    }
    val result = timeVector.zip(diffedTs)

    val spark = dataset.sparkSession
    import spark.implicits._

    val dataFrame = spark.createDataset(result).toDF($(timeCol), $(timeSeriesCol))

    dataFrame
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema
  }
}

object Differentiated {
  /**
    * Difference a vector with respect to the m-th prior element. Size-preserving by leaving first
    * `m` elements intact. This is the inverse of the `inverseDifferences` function.
    * @param ts Series to difference
    * @param destTs Series to store the differenced values (and return for convenience)
    * @param lag The difference lag (e.g. x means destTs(i) = ts(i) - ts(i - x), etc)
    * @param startIndex the starting index for the differencing. Must be at least equal to lag
    * @return the differenced vector, for convenience
    */
  def differencesAtLag(ts: Vector, destTs: Vector, lag: Int, startIndex: Int): Vector = {
    require(startIndex >= lag, "starting index cannot be less than lag")
    val diffedTs = if (destTs == null) ts.copy else destTs
    if (lag == 0) {
      diffedTs
    } else {
      val arr = diffedTs.toArray
      val n = ts.size
      var i = 0

      while (i < n) {
        // elements prior to starting point are copied over without modification
        arr(i) = if (i < startIndex) ts(i) else ts(i) - ts(i - lag)
        i += 1
      }
      diffedTs
    }
  }

  /**
    * Convenience wrapper around `differencesAtLag[Vector[Double], Vector[Double], Int, Int]`
    * @param ts vector to difference
    * @param lag the difference lag (e.g. x means destTs(i) = ts(i) - ts(i - x), etc)
    * @return the differenced vector, for convenience
    */
  def differencesAtLag(ts: Vector, lag: Int): Vector = {
    differencesAtLag(ts, null, lag, lag)
  }

  /**
    * Calculate an "inverse-differenced" vector of a given lag. Size-preserving by leaving first
    * `startIndex` elements intact. This is the inverse of the `differences` function.
    * @param diffedTs differenced vector that we want to inverse
    * @param destTs Series to store the added up values (and return for convenience)
    * @param lag The difference lag (e.g. x means destTs(i) = diffedTs(i) + destTs(i - x), etc)
    * @param startIndex the starting index for the differencing. Must be at least equal to lag
    * @return the inverse differenced vector, for convenience
    */
  def inverseDifferencesAtLag(diffedTs: Vector, destTs: Vector, lag: Int,
                              startIndex: Int): Vector = {
    require(startIndex >= lag, "starting index cannot be less than lag")
    val addedTs = if (destTs == null) diffedTs.copy else destTs
    if (lag == 0) {
      addedTs
    } else {
      val n = diffedTs.size
      var i = 0

      val arr = addedTs.toArray
      while (i < n) {
        // elements prior to starting point are copied over without modification
        arr(i) = if (i < startIndex) diffedTs(i) else diffedTs(i) + addedTs(i - lag)
        i += 1
      }
      addedTs
    }
  }

  /**
    * Convenience wrapper around `inverseDifferencesAtLag[Vector[Double], Vector[Double], Int, Int]`
    * @param diffedTs differenced vector that we want to inverse
    * @param lag the difference lag (e.g. x means destTs(i) = ts(i) - ts(i - x), etc)
    * @return the inverse differenced vector, for convenience
    */
  def inverseDifferencesAtLag(diffedTs: Vector, lag: Int): Vector = {
    inverseDifferencesAtLag(diffedTs, null, lag, lag)
  }

  /**
    * Performs differencing of order `d`. This means we recursively difference a vector a total of
    * d-times. So that d = 2 is a vector of the differences of differences. Note that for each
    * difference level, d_i, the element at ts(d_i - 1) corresponds to the value in the prior
    * iteration.
    * @param ts time series to difference
    * @param d order of differencing
    * @return a vector of the same length differenced to order d
    */
  def differencesOfOrderD(ts: Vector, d: Int): Vector = {
    // we create 2 copies to avoid copying with every call, and simply swap them as necessary
    // for higher order differencing
    var (diffedTs, origTs) = (ts.copy, ts.copy)
    var swap: Vector = null
    for (i <- 1 to d) {
      swap = origTs
      origTs = diffedTs
      diffedTs = swap
      differencesAtLag(origTs, diffedTs, 1, i)
    }
    diffedTs
  }

  /**
    * Inverses differencing of order `d`.
    * @param diffedTs time series to reverse differencing process
    * @param d order of differencing
    * @return a vector of the same length, which when differenced to order ts, yields the original
    *         vector provided
    */
  def inverseDifferencesOfOrderD(diffedTs: Vector, d: Int): Vector = {
    val addedTs = diffedTs.copy
    for (i <- d to 1 by -1) {
      inverseDifferencesAtLag(addedTs, addedTs, 1, i)
    }
    addedTs
  }
}
