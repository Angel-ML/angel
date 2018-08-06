/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.spark.ml.util

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row}

/**
 * DataSaver saves DataFrame to HDFS/LOCAL, all fields in DataFrame connect with a space
 *
 */
object DataSaver {
  def save(df: DataFrame, path: String): Unit = {
    df.printSchema()
    df.show(5)
    println(s"save data count: ${df.count()}")

    val rdd = df.rdd.map(row2Array)
    rdd.count()

    val outputPath = new Path(path)
    val conf = rdd.context.hadoopConfiguration

    val fs = outputPath.getFileSystem(conf)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }

    rdd.map(_.mkString(" ")).saveAsTextFile(path)
  }

  private def row2Array(row: Row): Array[String] = {
    val result = ArrayBuffer[String]()
    for (i <- 0 until row.length) {
      if (row.get(i).isInstanceOf[Vector]) {
        result ++= row.getAs[Vector](i).toArray.map(_.toString)
      } else if (row.get(i) == null) {
        result += "null"
      } else {
        val item = row.get(i) match {case x: java.lang.Object => x.toString}
        result += item
      }
    }
    result.toArray
  }
}
