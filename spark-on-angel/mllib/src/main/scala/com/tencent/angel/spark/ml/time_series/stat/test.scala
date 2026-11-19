package com.tencent.angel.spark.ml.time_series.stat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression => MLLR, LogisticRegressionModel => MLLRModel}

object test {
  def main(args: Array[String]): Unit = {

    val mode = "local"

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(
      (1, "2014/01/01 23:00:01"),
      (2, "2014/11/31 12:40:32"),
      (3, "2016/12/29 09:54:00"),
      (4, "2016/05/09 10:12:43"))
      .toDF("id", "date")

    val res = df.select($"id", $"date", unix_timestamp($"date", "yyyy/MM/dd HH:mm:ss").as("timestamp"))

    res.show()




  }

}
