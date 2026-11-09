package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.util.{DataLoader, MAX_INT_STR, ParamKeys}
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}

object transform2libSVMCluster {
  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val output = params.getOrElse("output", null)
    val featureCols = params.getOrElse("featureCols", null)
    val labelCol = params.getOrElse(ParamKeys.LABEL_COL, MAX_INT_STR).toInt

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")
    var trainingDFtable = DataLoader.loadTable(sc, input, partitionNum, sampleRate, labelCol + "," + featureCols, sep)

    val featuresCols = trainingDFtable.columns

    trainingDFtable = trainingDFtable.select(featuresCols.map(name => col(name).cast(DoubleType)): _*)

    val data = trainingDFtable.rdd.map { line =>
      val colsArray = line.toSeq.toArray.map { b => b.asInstanceOf[Double] }
      val features = Vectors.dense(colsArray.tail)
      val label = colsArray(0)
      LabeledPoint(label, features)
    }

    val confHadoop = sc.hadoopConfiguration
    val outputPath = new Path(output)
    val fs = outputPath.getFileSystem(confHadoop)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }

    MLUtils.saveAsLibSVMFile(data, output)
    sc.stop()

  }
}
