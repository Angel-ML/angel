package com.tencent.angel.spark.ml.classification.OutlierTransformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.execution.stat.StatFunctions
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}


class OutlierTransformer(override val uid: String) extends Transformer {
  def this() = this(Identifiable.randomUID("OutlierTransformer"))

  // parameter
  final val method = new Param[String](this, OutlierSoftenConstant.method,
    OutlierSoftenConstant.methodDoc,
    isValid = ParamValidators.inArray(OutlierSoftenConstant.supportedMethod))
  def setMethod(value: String): this.type = set(method, value)

  final val upperPercentage = new DoubleParam(this, OutlierSoftenConstant.upperPercentage,
    OutlierSoftenConstant.upperPerDoc)

  def setUpperPercentage(value: Double): this.type = set(upperPercentage, value)

  final val lowerPercentage = new DoubleParam(this, OutlierSoftenConstant.lowerPercentage,
    OutlierSoftenConstant.lowerPerDoc)

  def setLowerPercentage(value: Double): this.type = set(lowerPercentage, value)

  final val upperThreshold = new DoubleParam(this, OutlierSoftenConstant.upperThreshold,
    OutlierSoftenConstant.upperThresholdDoc)

  def setUpperThreshold(value: Double): this.type = set(upperThreshold, value)

  final val lowerThreshold = new DoubleParam(this, OutlierSoftenConstant.lowerThreshold,
    OutlierSoftenConstant.lowerThresholdDoc)

  def setLowerThreshold(value: Double): this.type = set(lowerThreshold, value)

  final val colNames = new Param[Array[String]](this, OutlierSoftenConstant.colNames,
    OutlierSoftenConstant.colNames)

  def setColNames(value: Array[String]): this.type = set(colNames, value)

  final val softenOutlier = new Param[Boolean](this, OutlierSoftenConstant.softenOutlier,
    OutlierSoftenConstant.softenOutlier)

  def setSoftenOutlier(value: Boolean): this.type = set(softenOutlier, value)


  override def transform(dataset: Dataset[_]): DataFrame = {

    val df = dataset.toDF()
    $(method) match {
      case OutlierSoftenConstant.zScore => zScoreMethod(df, $(colNames))
      case OutlierSoftenConstant.percentage =>
        percentageMethod(df, $(colNames), $(upperPercentage), $(lowerPercentage))
      case OutlierSoftenConstant.threshold =>
        thresholdMethod(df, $(colNames), $(upperThreshold), $(lowerThreshold))
      case _ => throw new Exception(OutlierSoftenConstant.method)
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  private def thresholdMethod(data: DataFrame, colName: String,
                              upper: Double, lower: Double): DataFrame = {
    if ($(softenOutlier)) {
      val transformUdf = udf { (v: Double) =>
        if (v > upper) {
          upper
        } else if (v < lower) {
          lower
        } else v
      }
      data.withColumn(colName, transformUdf(data(colName)))
    } else {
      data.where(s"$colName <= $upper and $colName >= $lower")
    }
  }

  private def thresholdMethod(data: DataFrame, colNames: Array[String],
                              upper: Double, lower: Double): DataFrame = {
    var out = data
    colNames.foreach(col => out = thresholdMethod(out, col, upper, lower))
    out
  }

  private def thresholdMethod(data: DataFrame, colNames: Array[String],
                              uppers: Array[Double], lowers: Array[Double]): DataFrame = {
    var out = data
    colNames.indices.foreach(i => out = thresholdMethod(out, colNames(i), uppers(i), lowers(i)))
    out
  }

  /**
    * Assume data conform to Normal distribution.
    * All the values outside [mean - 3 * sd, mean + 3 * sd] will be soften to $mean \pm 3 * sd$.
    * @param data data
    * @param colNames colNames
    * @return
    */
  private def zScoreMethod(data: DataFrame, colNames: Array[String]): DataFrame = {
    val stat = data.describe(colNames: _*).collect()
    // stat(1): get a mean Row; stat(2): get a sd Row
    val means = stat(1).toSeq.drop(1).toArray.map(_.toString.toDouble)
    val stds = stat(2).toSeq.drop(1).toArray.map(_.toString.toDouble)
    val uppers = means.indices.map(i => means(i) + 3 * stds(i)).toArray
    val lowers = means.indices.map(i => means(i) - 3 * stds(i)).toArray
    thresholdMethod(data, colNames, uppers, lowers)
  }

  private def percentageMethod(data: DataFrame, colNames: Array[String],
                               upper: Double, lower: Double): DataFrame = {
    val boundValues =
      StatFunctions.multipleApproxQuantiles(data, colNames.toSeq, Seq(upper, lower), 1e-5)
    val uppers = boundValues.map(_(0)).toArray
    val lowers = boundValues.map(_(1)).toArray
    thresholdMethod(data, colNames, uppers, lowers)
  }

  override def transformSchema(schema: StructType): StructType = schema
}
