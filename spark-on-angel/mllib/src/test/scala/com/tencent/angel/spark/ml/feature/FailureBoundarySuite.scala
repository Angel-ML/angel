package com.tencent.angel.spark.ml.feature

import com.tencent.angel.spark.ml.graph.algo.rank.commonFriendsRecall.CommonFriendsRecall
import com.tencent.angel.spark.ml.time_series.feature.TimeSeriesFeatures
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class FailureBoundarySuite extends AnyFunSuite {
  test("time series UDFs survive Spark actions and declare all feature columns") {
    val spark = SparkSession.builder.master("local[2]").appName("TimeSeriesBoundary")
      .config("spark.ui.enabled", "false").getOrCreate()
    try {
      val session = spark
      import session.implicits._
      val data = Seq(Tuple1(Vectors.dense(1, 2, 3, 4, 5)), Tuple1(Vectors.dense(2, 4, 6, 8, 10))).toDF("series")
      val transformer = new TimeSeriesFeatures().setSeriesColName("series").setMaxBin(2).setC3Lag(2)
      val transformed = transformer.transform(data.repartition(2))
      assert(transformed.schema.fieldNames.toSeq == transformer.transformSchema(data.schema).fieldNames.toSeq)
      assert(transformed.schema.size == 26)
      val rows = transformed.collect().sortBy(_.getAs[Double]("mean"))
      assert(rows.length == 2)
      assert(rows(0).getAs[Double]("maximum") == 5)
      assert(rows(0).getAs[Double]("minimum") == 1)
      assert(rows(0).getAs[Double]("mean") == 3)
      assert(rows(0).getAs[Double]("variance") == 2.5)
      assert(rows(0).getAs[Double]("median") == 3)
      assert(rows(0).getAs[Double]("sumOfSquare") == 55)
      assert(rows(0).getAs[Double]("c3") == 15)
      assert(rows(1).getAs[Double]("mean") == 6)
    } finally { spark.stop() }
  }

  test("one-way graph has no mutual-friend recommendations") {
    val spark = SparkSession.builder.master("local[2]").appName("EmptyRecallBoundary")
      .config("spark.ui.enabled", "false").getOrCreate()
    try {
      val session = spark
      import session.implicits._
      val edges = Seq((1L, 2L), (2L, 3L)).toDF("src", "dst")
      val model = new CommonFriendsRecall().setPartitionNum(2)
      val result = model.transform(edges)
      assert(result.count() == 0)
      assert(result.schema.fieldNames.toSeq == model.transformSchema(edges.schema).fieldNames.toSeq)
    } finally { spark.stop() }
  }
}
