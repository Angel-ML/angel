package com.tencent.angel.spark.ml.featurePreprocess.transform2libSVM

import com.tencent.angel.spark.ml.util.DataLoader
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}


object transform2libSVMLocal {
  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/mlTestData/iris.csv" //xxxx.txt,poker_hand.csv
    val labelCol = 4
    val partitionNum = 1
    val sampleRate = 1
    val featureCols = "0-3"
    val sep = ","
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/libSVM"

    // spark configure
    // SparkContext must be create here.
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")
    var trainingDFtable = DataLoader.loadTable(sc, input, partitionNum, sampleRate, labelCol + "," + featureCols, sep)

    val featuresCols = trainingDFtable.columns

    trainingDFtable = trainingDFtable.select(featuresCols.map(name => col(name).cast(DoubleType)): _*)

    trainingDFtable.show(5)
    println(trainingDFtable.schema)


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
