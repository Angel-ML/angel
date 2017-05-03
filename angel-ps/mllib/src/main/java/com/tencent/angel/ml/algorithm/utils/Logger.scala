package com.tencent.angel.ml.algorithm.utils

/**
  * Created by andyyehoo on 17/5/3.
  */
class Logger {
  def info(s:String){System.out.println(s)}
}

object Logger{
  def apply() = {
    println("this is object apply")
    new Logger()
  }
}