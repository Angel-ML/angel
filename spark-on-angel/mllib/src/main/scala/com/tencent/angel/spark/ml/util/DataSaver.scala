package com.tencent.angel.spark.ml.util

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

object DataSaver {

  private val DELIMITER = "delimiter"
  private val HEADER = "header"


  def save(df: DataFrame, path: String, sep: String): Unit = {

    val rdd = df.rdd.map(row2Array)
    if (path.startsWith(HDFS_PREFIX) || df.sqlContext.sparkContext.isLocal) {
      // hdfs or local path
      HDFSUtils.save(rdd, path, sep)
    } else {
      throw new Exception(s"Wrong path: $path")
    }
  }


  private def row2Array(row: Row): Array[String] = {
    val result = ArrayBuffer[String]()
    for (i <- 0 until row.length) {
      if (row.get(i).isInstanceOf[Vector]) {
        result ++= row.getAs[Vector](i).toArray.map(_.toString)
      } else if (row.get(i) == null) {
        result += "null"
      } else {
        val item = row.get(i) match {
          case x: java.lang.Object => x.toString
        }
        result += item
      }
    }
    result.toArray
  }

  /**
   * 保存RDD结果
   *
   * @param dataRdd
   * @param path
   */
  def save(dataRdd: RDD[Array[String]], path: String, sep: String) {
    // hdfs or local path
    HDFSUtils.save(dataRdd, path, sep)
  }

}
