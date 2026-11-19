package com.tencent.angel.spark.ml.util

import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

/**
 * Created by isakjiang on 2022-12-27.
 */

/**
 * DataFrameUtil
 *
 * implement some common DataFrame Transformation in this object
 *
 */
object DataFrameUtil {

  def selectColsAsDoubles(input: DataFrame, cols: Array[Int]): DataFrame = {
    val colNames = cols.map { id => input.columns(id) }
    input.select(colNames.map(name => col(name).cast(DoubleType)): _*)
  }

  def removeNullString(totalText: DataFrame, textColName: String): DataFrame = {
    totalText.filter(textColName + " is not null and "+textColName+ " <> ' ' and "+textColName+ " <> '' ")
  }

}
