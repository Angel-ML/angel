package com.tencent.angel.ml.auto.parameter

import scala.util.Random

/**
  * Search space with discrete values
  *
  * @param name: Name of the parameter
  * @param values: List of all possible values
  */
class DiscreteSpace[T](override val name: String, values: List[T], seed: Int = 100) extends ParamSpace(name) {

  val rd = new Random(seed)

  def getValues: List[T] = values

  def numValues: Int = values.length

  def toGridSearch: ParamSpace = this

  def toRandomSpace: ParamSpace = this

  def sample(size: Int): List[T] = {
    Random.shuffle(values).take(size)
  }

  override def toString: String = {
    return s"DiscreteSpace[$name]: (${values mkString(",")})"
  }
}

object DiscreteSpace {

  def main(args: Array[String]): Unit = {
    val obj = new DiscreteSpace[Double]("test", List(1.0, 2.0, 3.0, 4.0, 5.0))
    println(obj.toString)
    println(obj.sample(2).toString())
  }
}
