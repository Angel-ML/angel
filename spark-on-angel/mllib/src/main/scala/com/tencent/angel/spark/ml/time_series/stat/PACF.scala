package com.tencent.angel.spark.ml.time_series.stat

import breeze.linalg.{DenseMatrix, Vector}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import  com.tencent.angel.spark.ml.util.UnivariateTimeSeriesUtil
import com.tencent.angel.spark.ml.util.MatrixUtil

trait PACFParams extends TimeSeriesParams {
  final val maxLag = new Param[Int](this, "maxLag", "max lag")
  def setMaxLag(value: Int): this.type = set(maxLag, value)
}

class PACF(override val uid: String) extends Transformer with PACFParams {

  def this() = this(Identifiable.randomUID("PACF"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val data = dataset.select($(timeCol), $(timeSeriesCol)).rdd.map {
     // case Row(time: String, value: Double) => (time, value)
      case Row(time: Double, value: Double) => (time, value)
      case Row(time: Int, value: Double) => (time.toDouble, value)
      case Row(time: Float, value: Double) => (time.toDouble, value)
      case Row(time: String, value: Double) => (time.toDouble, value)
    }.sortByKey().collect()

    val coeffs: Array[Double] = pacf(data)

    // calculate confidence interval for PACF (fixed ci)
    // Ref: https://stats.stackexchange.com/questions/58090/confidence-bands-in-case-of-fitting-arima-in-r
    // Ref: https://stats.stackexchange.com/questions/129052/acf-and-pacf-formula
    val ci = 1.96 / math.sqrt(data.length) // 1.96 for 95% confidence bands
    val tempVec = Array.fill(coeffs.length)(ci)

    val resultDF = coeffs.zip(tempVec).zipWithIndex
    val resRDD = dataset.sparkSession.sparkContext.parallelize(resultDF.map(x => Row(x._1._1, x._1._2, x._2.toDouble)))

    val structType = transformSchema(dataset.schema)

    dataset.sparkSession.createDataFrame(resRDD, structType)  }


  /**
    * Makes a Toeplitz matrix from the given time series, used in Toeplitz()
    */
  def matToeplitz(x: Vector[Double],
                  outputMat: DenseMatrix[Double], maxLag: Int): Unit = {

    val breezeOutputMat = outputMat
    for (iter <- 0 until maxLag) {
      breezeOutputMat(iter, 0) = x(iter)
      breezeOutputMat(0, iter) = x(iter)
    }
    if (maxLag > 1) {
      for (r <- 1 until maxLag) {
        for (c <- 1 until maxLag) {
          breezeOutputMat(r, c) = breezeOutputMat(r - 1, c - 1)
        }
      }
    }
  }

  /**
    * Makes a Toeplitz matrix from the given time series, used for calculating PACF
    */
  def Toeplitz(x: Vector[Double], maxLag: Int): DenseMatrix[Double] = {
    val desMat = new DenseMatrix(maxLag, maxLag, new Array[Double](maxLag * maxLag))
    matToeplitz(x, desMat, maxLag)

    desMat
  }


  /**
    * Calculate PACF using Durbin-Levinson recursion
    * Return: Array of size maxLag + 1 with the last item being the standard error for plotting confidence interval
    * (i.e. mapping between the sample autocorrelations and the partial autocorrelations).
    * Ref: https://stats.stackexchange.com/questions/129052/acf-and-pacf-formula
    */
  def pacf(data: Array[(Double, Double)]): Array[Double] = {
    val dataVec = Vectors.dense(data.map(x => x._2))
    // get ACF results and prepend "1" (which is rho_zero)
    val rhoZero = Array(1.0)
    var autoCorrs = UnivariateTimeSeriesUtil.autocorr(dataVec, $(maxLag)).toArray
    autoCorrs = rhoZero ++ autoCorrs

    val dataVector = Vectors.dense(autoCorrs)
    val coeffs = new Array[Double]($(maxLag))
    var k = 1
    while (k < $(maxLag) + 1){
      // create Toeplitz matrix
      val tempVec1 = MatrixUtil.toBreeze(dataVector)(0 until k)
      val tempMat = tempVec1.toDenseVector
      val mat = Toeplitz(tempMat, k)
      // create rho_1 to rho_k
      val tempVec2 = MatrixUtil.toBreeze(dataVector)(1 until k + 1)
      val rightVec = tempVec2.toDenseVector
      // solve for the result; the last index of result is PACF
      val sol = mat \ rightVec
      coeffs(k-1) = sol(k-1)
      k += 1
    }
    coeffs
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(StructField("PACF", DoubleType),
      StructField("CI", DoubleType),
      StructField("index", DoubleType)))
  }
}
