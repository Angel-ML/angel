package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.fpm.prefixSpanParallel.PrefixSpanPara
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.{DataLoader, LogUtils, ParamKeys}
import org.apache.spark.ml.fpm.PrefixSpan
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import scala.collection.mutable

object PrefixSpanParaCluster {

  def main(args: Array[String]) {
    LogUtils.logTime("PrefixSpan start...")
    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse(ParamKeys.OUTPUT, null)
    val partitionNum = params.getOrElse("partitionNum", "4").toInt

    val sepData = params.getOrElse("sepData", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val sepSeq = params.getOrElse("sepSeq", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val minSupport = params.getOrElse("minSupport", "0.001").toDouble
    val maxLength = params.getOrElse("maxLength", "10").toInt
    val dataIndex = params.getOrElse("dataIndex", "0").toInt
    val indexIndex = params.getOrElse("indexIndex", "-1").toInt

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val spark = SparkSession.builder.appName(s"${this.getClass.getSimpleName}").getOrCreate()

    if (indexIndex != -1 && indexIndex != null) {
      runPara(input, output, minSupport, maxLength, partitionNum, sepData, sepSeq, indexIndex, dataIndex)
    }
    else {
      runOri(input, output, minSupport, maxLength, partitionNum, sepData, sepSeq, dataIndex)
    }

    spark.close()

    def runPara(input: String, output: String, minSupport: Double, maxLength: Int, partitionNum: Int,
                sepData: String, sepSeq: String, indexIndex: Int, dataIndex: Int): Unit = {

      val dataDF = DataLoader.loadTable(sc, input, partitionNum, 1, null, sepData)
      val columnsNames = dataDF.columns

      val dataRDD = dataDF.select(columnsNames(indexIndex), columnsNames(dataIndex)).rdd
        .filter(row => !row.anyNull)
        .map { row => (row.get(0).toString, row.get(1).toString) }
        .filter(f => (!f._1.isEmpty) && (!f._2.isEmpty))

      val prefixSpan = new PrefixSpanPara()
        .setMinSupport(minSupport)
        .setMaxPatternLength(maxLength)

      val result = dataRDD.groupByKey().flatMap { case (key, value) =>
        val dataSeq = value.toArray.map { t =>
          t.split(sepSeq).map(x => x.split("\\|"))
        }
        prefixSpan.run(dataSeq).map(x => (key, x._1, x._2.toString))
      }

      def transformSchema(): StructType = {
        StructType(Seq(
          StructField("index", StringType, nullable = false),
          StructField("itemset", StringType, nullable = false),
          StructField("freq", StringType, nullable = false)
        ))
      }

      val tempRe = dataDF.sparkSession.createDataFrame(result.map(x => Row.fromSeq(Seq[Any](x._1, x._2, x._3))), transformSchema())
      GraphIO.save(tempRe, output, sepData)
    }

    def runOri(input: String,
               output: String,
               minSupport: Double,
               maxLength: Int,
               partitionNum: Int,
               sepData: String,
               sepSeq: String,
               dataIndiex: Int): Unit = {
      val dataDF = DataLoader.loadDataItemsWithIndex(input, partitionNum, sepData, sepSeq, dataIndiex, true)(SparkContext.getOrCreate())

      var result = new PrefixSpan()
        .setMinSupport(minSupport)
        .setSequenceCol("items")
        .setMaxPatternLength(maxLength)
        .setMaxLocalProjDBSize(32000000)
        .findFrequentSequentialPatterns(dataDF)

      def arrayToString(s: mutable.WrappedArray[mutable.WrappedArray[String]]): String = {
        s.map(x => x.mkString("|")).mkString("#")
      }

      val arrayToStringUDF: UserDefinedFunction = udf[String, mutable.WrappedArray[mutable.WrappedArray[String]]](arrayToString)

      result = result.select(arrayToStringUDF(col("sequence")).as("itemset"), col("freq")).repartition(partitionNum)
      GraphIO.save(result, output, sepData)
    }

  }
}

