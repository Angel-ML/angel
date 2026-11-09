package com.tencent.angel.spark.ml.fpm.PrefixSpan

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.fpm.PrefixSpan

object PrefixSpanExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    import spark.implicits._
    
    val temInput = Seq(
      Seq(Seq("a", "b"), Seq("c")),
      Seq(Seq("a"), Seq("c", "b"), Seq("a", "b")),
      Seq(Seq("a", "b"), Seq("e")),
      Seq(Seq("f")))
    
    val df = temInput.toDF("sequence")
    df.schema.printTreeString()
    df.show()
    println(df.schema)


    val result = new PrefixSpan()
      .setMinSupport(0.5)
      .setSequenceCol("sequence")
      .setMaxPatternLength(5)
      .setMaxLocalProjDBSize(32000000)
      .findFrequentSequentialPatterns(df)
      .show()

  }
}
