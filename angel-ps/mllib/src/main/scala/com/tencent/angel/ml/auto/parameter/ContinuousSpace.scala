package com.tencent.angel.ml.auto.parameter

import com.tencent.angel.ml.auto.utils.Distribution
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  *
  * @param name: Name of the parameter
  * @param lower: Start of the continuous space included.
  * @param upper: End of the continuous space included.
  * @param num: Sampling count if possible.
  * @param seed
  */
class ContinuousSpace(override val name: String, lower: Double, upper: Double, num: Int,
                      distribution: Distribution.Value = Distribution.LINEAR, seed: Int = 100) extends ParamSpace(name) {
  val LOG: Log = LogFactory.getLog(classOf[ContinuousSpace])

  val rd = new Random(seed)
  val values: List[Double] = calValues

  def calValues(): List[Double] = {
    var ret: ListBuffer[Double] = ListBuffer[Double]()
    distribution match {
      case Distribution.LINEAR =>
        val interval: Double = (upper - lower) / (num - 1)
        (0 until num).foreach { i =>
          ret += lower + i * interval
        }
      case _ => LOG.info(s"Distribution $distribution not supported")
    }
    ret.toList
  }

  def getLower: Double = lower

  def getUpper: Double = upper

  def getValues: List[Double] = values

  def numValues: Int = num

  def toGridSearch: ParamSpace = this

  def toRandomSpace: ParamSpace = this

  def sample(size: Int): List[Double] = {
    Random.shuffle(values).take(size)
  }

  override def toString: String = {
    return s"ContinuousSpace[$name]: (${values mkString(",")})"
  }

}

object ContinuousSpace {

  def main(args: Array[String]): Unit = {
    val obj = new ContinuousSpace("test", 0, 10, 5)
    println(obj.toString)
    println(obj.sample(2).toString())
  }
}