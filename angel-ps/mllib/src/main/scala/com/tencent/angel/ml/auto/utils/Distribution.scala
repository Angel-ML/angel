package com.tencent.angel.ml.auto.utils

object Distribution extends Enumeration {

  type Distribution = Value

  val LINEAR = Value("1")

  def checkExists(distribution: String): Boolean = this.values.exists(_.toString == distribution)

  def printAll(): Unit = this.values.foreach(println)
}
