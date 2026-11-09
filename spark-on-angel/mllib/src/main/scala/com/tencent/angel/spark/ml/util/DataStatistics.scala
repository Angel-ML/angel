package com.tencent.angel.spark.ml.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * Open-source replacement for the internal statistics reporter.
 * The 2.5.0 implementation forwarded job statistics to Tencent-internal
 * monitoring services; these hooks are kept as no-ops so callers remain
 * source compatible.
 */
object DataStatistics {
  def calcRDDSize(rdd: RDD[String], inputName: String): Unit = {}

  def calcRDDSize(df: DataFrame, inputName: String): Unit = {}

  def setAppName(name: String): Unit = {}

  def setDataPartitionNum(partitionNum: Int): Unit = {}

  def setPSPartitionNum(psPartitionNum: Int): Unit = {}
}
