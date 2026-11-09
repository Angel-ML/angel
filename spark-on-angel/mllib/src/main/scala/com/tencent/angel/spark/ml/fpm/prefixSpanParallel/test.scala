package com.tencent.angel.spark.ml.fpm.prefixSpanParallel

import com.tencent.angel.spark.ml.util.LogUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.angel.PrefixSpan

object test {

    def main(args: Array[String]) {
      LogUtils.logTime("PrefixSpan start...")
      val mode = "local"
      val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(mode)
      val sc = new SparkContext(conf)
      sc.setLogLevel("WARN")

      val input = "data/mlTestData/PrefixSpan_data_test"
      val output = "data/output/output"

      val minSupport = 0.01
      val maxLength = 10
      val partitionNum = 1
      val dataIndiex = 0
      val sep = ","

      val spark = SparkSession.builder.appName(s"${this.getClass.getSimpleName}").getOrCreate()



      val sequences = sc.parallelize(Seq(
        Array(Array(1, 2), Array(3)),
        Array(Array(1), Array(3, 2), Array(1, 2)),
        Array(Array(1, 2), Array(5)),
        Array(Array(1),Array(6))
      ), 2).cache()
      val prefixSpan = new PrefixSpan()
        .setMinSupport(0.5)
        .setMaxPatternLength(5)
      val model = prefixSpan.run(sequences)

      println("===end===")
      model.freqSequences.collect().foreach { freqSequence =>
        println(
          s"${freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")}," +
            s" ${freqSequence.freq}")
      }



      spark.close()

    }




}
