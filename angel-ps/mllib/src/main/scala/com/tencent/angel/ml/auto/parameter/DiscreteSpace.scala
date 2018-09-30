package com.tencent.angel.ml.auto.parameter

import scala.util.Random

/**
  * Search space with discrete values
  *
  * @param name: Name of the parameter
  * @param values: List of all possible values
  */
class DiscreteSpace[T: Numeric](override val name: String, values: List[T], seed: Int = 100)
  extends ParamSpace[T](name) {

  val rd = new Random(seed)

  def getValues: List[T] = values

  def numValues: Int = values.length

  def toGridSearch: ParamSpace[T] = this

  def toRandomSpace: ParamSpace[T] = this

  override def sample(size: Int): List[T] = rd.shuffle(values).take(size)

  override def sample: T = values(rd.nextInt(numValues))

  override def toString: String = s"DiscreteSpace[$name]: (${values mkString(",")})"
}

object DiscreteSpace {

  def main(args: Array[String]): Unit = {
    val obj = new DiscreteSpace[Float]("test", List(1.0f, 2.0f, 3.0f, 4.0f, 5.0f))
    println(obj.toString)
    println(obj.sample(2).toString())
  }
}
