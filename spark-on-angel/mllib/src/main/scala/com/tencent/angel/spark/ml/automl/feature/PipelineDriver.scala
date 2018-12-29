package com.tencent.angel.spark.ml.automl.feature

import org.apache.spark.sql.SparkSession

object PipelineDriver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    val inputPath = "data/"

    val inputDF = DataLoader.load(spark, "libsvm", "input/data")


  }

}
