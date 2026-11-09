package com.tencent.angel.spark.ml.time_series.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}


trait GenerateTimeSeriesFeatureParams extends Params {
  final val seriesColName = new Param[String](this, "seriesColID",
    "The column index of time series")

  final val maxBin = new IntParam(this, "maxBin",
    "The number of bins in computing binned entropy")

  final val c3Lag = new IntParam(this, "c3Lag",
    "The lag parameter in computing c3")

  def setSeriesColName(value: String): GenerateTimeSeriesFeatureParams.this.type =
    set(seriesColName, value)

  def setMaxBin(value: Int): GenerateTimeSeriesFeatureParams.this.type =
    set(maxBin, value)

  def setC3Lag(value: Int): GenerateTimeSeriesFeatureParams.this.type =
    set(c3Lag, value)
}

class TimeSeriesFeatures(override val uid: String)
  extends Transformer with GenerateTimeSeriesFeatureParams with TimeSeriesFeatureUdfs {
  /*
  A transformer that can be integrated into a spark ml pipeline,
  which can generate time series features.
  The input dataframe should be with a column
  containing time series data in spark.ml.linalg.Vector format.
   */
  def this() = this(Identifiable.randomUID("GenerateTimeSeriesFeature"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    val seriesColumn = dataset.col($(seriesColName))

    dataset.select(
      col("*"),
      maximum(seriesColumn).as("maximum"),
      minimum(seriesColumn).as("minimum"),
      mean(seriesColumn).as("mean"),
      variance(seriesColumn).as("variance"),
      std(seriesColumn).as("std"),
      skewness(seriesColumn).as("skewness"),
      kurtosis(seriesColumn).as("kurtosis"),
      median(seriesColumn).as("median"),
      range(seriesColumn).as("range"),
      sumOfSquare(seriesColumn).as("sumOfSquare"),
      meanChange(seriesColumn).as("meanChange"),
      countAboveMean(seriesColumn).as("countAboveMean"),
      countBelowMean(seriesColumn).as("countBelowMean"),
      firstLocationOfMaximum(seriesColumn).as("firstLocationOfMaximum"),
      lastLocationOfMaximum(seriesColumn).as("lastLocationOfMaximum"),
      firstLocationOfMinimum(seriesColumn).as("firstLocationOfMinimum"),
      lastLocationOfMinimum(seriesColumn).as("lastLocationOfMinimum"),
      hasDuplicate(seriesColumn).as("hasDuplicate"),
      hasDuplicateMaximum(seriesColumn).as("hasDuplicateMaximum"),
      hasDuplicateMinimum(seriesColumn).as("hasDuplicateMinimum"),
      longestStrikeAboveMean(seriesColumn).as("longestStrikeAboveMean"),
      longestStrikeBelowMean(seriesColumn).as("longestStrikeBelowMean"),
      ratioOfReoccurringValuesToAllValues(seriesColumn).as("ratioOfReoccurringValuesToAllValues"),
      binnedEntropy(seriesColumn, lit(${maxBin})).as("binnedEntropy"),
      c3(seriesColumn, lit(${c3Lag})).as("c3"))
  }

  override def transformSchema(schema: StructType): StructType = {

    schema.add(StructField("maximum", DoubleType))
    schema.add(StructField("minimum", DoubleType))
    schema.add(StructField("mean", DoubleType))
    schema.add(StructField("variance", DoubleType))
    schema.add(StructField("std", DoubleType))
    schema.add(StructField("skewness", DoubleType))
    schema.add(StructField("kurtosis", DoubleType))
    schema.add(StructField("median", DoubleType))
    schema.add(StructField("range", DoubleType))
    schema.add(StructField("sumOfSquare", DoubleType))
    schema.add(StructField("meanChange", DoubleType))
    schema.add(StructField("countAboveMean", IntegerType))
    schema.add(StructField("countBelowMean", IntegerType))
    schema.add(StructField("firstLocationOfMaximum", DoubleType))
    schema.add(StructField("lastLocationOfMaximum", DoubleType))
    schema.add(StructField("firstLocationOfMinimum", DoubleType))
    schema.add(StructField("lastLocationOfMinimum", DoubleType))
    schema.add(StructField("hasDuplicate", IntegerType))
    schema.add(StructField("hasDuplicateMaximum", IntegerType))
    schema.add(StructField("hasDuplicateMinimum", IntegerType))
    schema.add(StructField("longestStrikeAboveMean", IntegerType))
    schema.add(StructField("longestStrikeBelowMean", IntegerType))
    schema.add(StructField("ratioOfReoccurringValuesToAllValues", DoubleType))
    schema.add(StructField("binnedEntropy", DoubleType))
    schema.add(StructField("c3", DoubleType))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
