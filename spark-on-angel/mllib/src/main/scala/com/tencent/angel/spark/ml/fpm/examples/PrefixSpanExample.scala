package com.tencent.angel.spark.ml.fpm.examples

import org.apache.spark.sql.SparkSession

object PrefixSpanExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    import spark.implicits._
    
    val temInput = Seq(
      Seq(Seq(1, 2), Seq(3)),
      Seq(Seq(1), Seq(3, 2), Seq(1, 2)),
      Seq(Seq(1, 2), Seq(5)),
      Seq(Seq(6)))
    
    val df = temInput.toDF("sequence")
    df.schema.printTreeString()
    df.show()
    
  }
}
