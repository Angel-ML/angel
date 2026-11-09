package com.tencent.angel.spark.ml.fpm.association

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.association.Apriori
import org.apache.spark.sql.SparkSession

/**
  * Created by endy on 2019/3/22.
  */
object AprioriSuite  {
  def main(args: Array[String]): Unit = {
    val mode = "local"
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    import spark.implicits._

    // $example on$
    val dataset = spark.createDataset(Seq(
      "1 2 5",
      "1 2 3 5",
      "1 2")
    ).map(t => t.split(" ")).toDF("items")
    dataset.show()

    val result = new Apriori()
      .setMaxLength(10)
      .setMinLength(1)
      .setMaxSupport(1.0)
      .setMinSupport(0.1)
      .setInputCol("items")
      .setOutputCols(Array("itemset", "frequency"))
      .transform(dataset)

    //print(result.count())
    result.show()
  }
}