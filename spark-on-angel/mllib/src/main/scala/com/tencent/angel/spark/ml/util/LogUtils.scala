package com.tencent.angel.spark.ml.util

import java.text.SimpleDateFormat
import java.util.Date

object LogUtils {
  def logTime(msg: String): Unit = {
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date)
    println(s"[$time] $msg")
  }
}
