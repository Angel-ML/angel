package com.tencent.angel.spark.ml.graph.utils

import com.tencent.tdw.spark.toolkit.tdw.TDWFunctions
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object GraphIO {

  private val DELIMITER = "delimiter"
  private val HEADER = "header"

  private val int2Long = udf[Long, Int](_.toLong)
  private val string2Long = udf[Long, String](_.toLong)
  private val int2Float = udf[Float, Int](_.toFloat)
  private val long2Float = udf[Float, Long](_.toFloat)
  private val double2Float = udf[Float, Double](_.toFloat)
  private val string2Float = udf[Float, String](_.toFloat)

  def convert2Float(df: DataFrame, structField: StructField, tmpSuffix: String): DataFrame = {
    val tmpName = structField.name + tmpSuffix
    structField.dataType match {
      case _: LongType =>
        df.withColumn(tmpName, long2Float(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: IntegerType =>
        df.withColumn(tmpName, int2Float(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: DoubleType =>
        df.withColumn(tmpName, double2Float(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: StringType =>
        df.withColumn(tmpName, string2Float(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: FloatType => df
      case t => throw new Exception(s"$t can't convert to Float")
    }
  }

  def convert2Long(df: DataFrame, structField: StructField, tmpSuffix: String): DataFrame = {
    val tmpName = structField.name + tmpSuffix
    structField.dataType match {
      case _: LongType => df
      case _: IntegerType =>
        df.withColumn(tmpName, int2Long(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case _: StringType =>
        df.withColumn(tmpName, string2Long(df(structField.name)))
          .drop(structField.name)
          .withColumnRenamed(tmpName, structField.name)
      case t => throw new Exception(s"$t can't convert to Long")
    }
  }

  def load(input: String, isWeighted: Boolean,
           srcIndex: Int = 0, dstIndex: Int = 1, weightIndex: Int = 2,
           sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    if (input.trim.startsWith("tdw://")) {
      var df = TDWFunctions.loadTable(ss, input)
      val columns = df.columns
      if (isWeighted) {
        df = df.select(columns(srcIndex), columns(dstIndex), columns(weightIndex))
          .withColumnRenamed(columns(srcIndex), "src")
          .withColumnRenamed(columns(dstIndex), "dst")
          .withColumnRenamed(columns(weightIndex), "weight")
        df.printSchema()
        df = convert2Float(df, df.schema(2), "_tmp")
      } else {
        df = df.select(columns(srcIndex), columns(dstIndex))
          .withColumnRenamed(columns(srcIndex), "src")
          .withColumnRenamed(columns(dstIndex), "dst")

      }
      df.printSchema()
      val schemaToLong = df.schema.take(2)
      schemaToLong.foreach { field =>
        df = convert2Long(df, field, "_tmp")
      }
      df.printSchema()
      df
    } else {
      val schema = if (isWeighted) {
        StructType(Seq(
          StructField("src", LongType, nullable = false),
          StructField("dst", LongType, nullable = false),
          StructField("weight", FloatType, nullable = false)
        ))
      } else {
        StructType(Seq(
          StructField("src", LongType, nullable = false),
          StructField("dst", LongType, nullable = false)
        ))
      }
      ss.read
        .option("sep", sep)
        .option("header", "false")
        .schema(schema)
        .csv(input)
    }
  }

  private def writeToTDW(df: DataFrame, url: String) {
    import com.tencent.tdw.spark.toolkit.tdw.{TDWProvider, TDWUtil}
    val dataRdd = df.rdd.map { row =>
      row.toSeq.toArray.map(_.toString)
    }
    val (groupName, dbName, tblName, priPart, subPart) = parseTDWOutUrl(url)
    val tdw = new TDWProvider(SparkContext.getOrCreate(), dbName, groupName)
    val tdwUtil = new TDWUtil(dbName, groupName)
    if (null == priPart) {
      tdw.saveToTable(dataRdd, tblName, "p_" + priPart, overwrite = true)
    } else if (null == subPart) {
      if (!tdwUtil.partitionExist(tblName, "p_" + priPart))
        tdwUtil.createListPartition(tblName, "p_" + priPart, priPart, 0)
      else
        tdwUtil.truncatePartition(tblName, "p_" + priPart)
      tdw.saveToTable(dataRdd.map { line =>
        val arr = new Array[String](line.length + 1)
        arr(0) = priPart
        System.arraycopy(line, 0, arr, 1, line.length)
        arr
      }, tblName, "p_" + priPart, overwrite = true)
    } else {
      if (!tdwUtil.partitionExist(tblName, "p_" + priPart))
        tdwUtil.createListPartition(tblName, "p_" + priPart, priPart, 0)
      if (!tdwUtil.partitionExist(tblName, "s_" + subPart))
        tdwUtil.createListPartition(tblName, "s_" + subPart, subPart, 1)
      else
        tdwUtil.truncatePartition(tblName, "p_" + priPart, "s_" + subPart)
      tdw.saveToTable(dataRdd.map { line =>
        val arr = new Array[String](line.length + 2)
        arr(0) = priPart
        arr(1) = subPart
        System.arraycopy(line, 0, arr, 2, line.length)
        arr
      }, tblName, "p_" + priPart, "s_" + subPart, overwrite = true)
    }
  }

  def parseTDWOutUrl(url: String): (String, String, String, String, String) = {
    val TDW_URL_REGEX = """^(?:tdw://)?(?:(\w+):)?(\w+)/(\w+)(?:/(\w+))?(?:/(\w+))?$""".r
    val TDW_URL_FORMAT = "[tdw://][groupName:]dbName/tableName[/priPart[/subPart]]"
    val GROUP_DEFAULT = "tl"
    TDW_URL_REGEX.unapplySeq(url.trim) match {
      case Some(List(g, db, tb, pri, sub)) =>
        val group = if (g == null) GROUP_DEFAULT else g
        (group, db, tb, pri, sub)
      case None => throw new Exception(s"invalid tdw url for output: got $url while $TDW_URL_FORMAT is required")
    }
  }

  def save(df: DataFrame, output: String, seq: String = "\t"): Unit = {
    df.printSchema()
    if (output.startsWith("tdw://")) {
      writeToTDW(df, output)
    } else {
      df.write
        .mode(SaveMode.Overwrite)
        .option(HEADER, "false")
        .option(DELIMITER, seq)
        .csv(output)
    }
  }

  def defaultCheckpointDir: Option[String] = {
    val sparkContext = SparkContext.getOrCreate()
    sparkContext.getConf.getOption("spark.yarn.stagingDir")
      .map { base =>
        new Path(base, s".sparkStaging/${sparkContext.getConf.getAppId}").toString
      }
  }
}