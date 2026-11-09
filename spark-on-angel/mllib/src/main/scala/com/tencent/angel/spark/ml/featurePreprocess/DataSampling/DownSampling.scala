package com.tencent.angel.spark.ml.featurePreprocess.DataSampling

import com.tencent.angel.spark.ml.util.Utils.checkValueEqual
import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.{SparkConf, SparkContext}

trait DownSamplingParams extends Params {

  final val sampleRatio = new DoubleParam(this, "sampleRatio",
    "The ratio of sample class num divide target class num", (x: Double) => x >= 1)

  def setSampleRatio(value: Double): this.type = set(sampleRatio, value)

  final val labelCol: Param[String] = new Param[String](this, "labelCol", "label column name")

  def setLabelCol(value: String): this.type = set(labelCol, value)

  final val sampledLabel = new Param[String](this, "sampledLabel", "label will over sample")

  def setSampledLabel(value: String): this.type = set(sampledLabel, value)

  final val targetLabel = new Param[String](this, "targetLabel",
    "target label, the target label count is less than sampled label value")

  def setTargetLabel(value: String): this.type = set(targetLabel, value)
}

class DownSampling(override val uid: String) extends Transformer with DownSamplingParams {
  def this() = this(Identifiable.randomUID("DownSampling"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val data = dataset.na.drop(Array($(labelCol)))

    val labelCountPair = data.groupBy($(labelCol)).count().collect()

    val labelDtype = dataset.schema($(labelCol)).dataType

    println(s"The label col is ${$(labelCol)}")
    println(s"label count: " +
      s"${labelCountPair.map(r => (r.get(0), r.get(1))).mkString("(", ", ", ")")}")
    println(s"Sampled label is: ${$(sampledLabel)}")
    println(s"Target label is: ${$(targetLabel)}")

    val sampledLabelCount = labelCountPair
      .filter { case Row(label: Any, _: Long) => checkValueEqual(labelDtype, label, $(sampledLabel)) }
      .map(x => x.get(1)).head.asInstanceOf[Long]

    val targetLabelCount = labelCountPair
      .filter { case Row(label: Any, _: Long) => checkValueEqual(labelDtype, label, $(targetLabel)) }
      .map(x => x.get(1)).head.asInstanceOf[Long]

    val ratio = sampledLabelCount / targetLabelCount.toDouble

    val sampleDF = if (ratio < $(sampleRatio)) {
      data.filter(col($(labelCol)) === $(sampledLabel))
    } else {
      data.filter(col($(labelCol)) === $(sampledLabel))
        .sample(withReplacement = false, $(sampleRatio) / ratio)
    }

    val otherDF = data.filter(col($(labelCol)) =!= $(sampledLabel))

    val result = sampleDF.union(otherDF)

    result
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema
}

object DownSampling {
  def main(args: Array[String]): Unit = {

    val mode = "local"
    val input = "data/mlTestData/iris_two_class_sample_test.csv"
    val output = "data/output/output"
    val partitionNum = 1
    val sep = ","
    val sampleRatio = 1
    val labelCol = 4
    val targetLabel = "0"
    val sampledLabel = "1"

    val sc = start(mode)
    val dataset = DataLoader.loadTable(sc, input, partitionNum, 1, "", sep)


    val dataCols = dataset.columns
    val labelColName = dataCols(labelCol)

    val DownSampling = new DownSampling()
      .setSampleRatio(sampleRatio)
      .setLabelCol(labelColName)
      .setTargetLabel(targetLabel)
      .setSampledLabel(sampledLabel)


    val result = DownSampling.transform(dataset)
    DataSaver.save(result, output, sep)

  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }

}