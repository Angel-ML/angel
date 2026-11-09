package com.tencent.angel.spark.ml.util

import org.apache.spark.sql.types.{DataType, NumericType, StringType}

object Utils {
  def checkValueEqual(dataType: DataType, v1: Any, v2: Any): Boolean = dataType match {
    case _: NumericType => v1.toString.toDouble == v2.toString.toDouble
    case _: StringType => v1.toString == v2.toString
    case _ => throw new IllegalArgumentException(s"Unsupported dtype ${dataType}")
  }

  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  // scalastyle:off classforname
  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
    // scalastyle:on classforname
  }

}

