package com.tencent.angel.graph.utils.io

import java.text.SimpleDateFormat
import java.util.Date

object Log {

  /**
    * println with date prefix
    */
  def withTimePrintln(info: String): Unit = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = dateFormat.format(new Date().getTime)
    println(s"[$time]$info")
  }
}
