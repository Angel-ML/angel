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


package com.tencent.angel.spark.ml.automl.tuner.parameter

import com.tencent.angel.spark.ml.automl.utils.{AutoMLException, Distribution}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  *
  * @param name: Name of the parameter
  * @param lower: Start of the continuous space included.
  * @param upper: End of the continuous space included.
  * @param num: Sampling count if possible.
  * @param seed
  */
class ContinuousSpace(
                       override val name: String,
                       var lower: Double,
                       var upper: Double,
                       var num: Int,
                       distribution: Distribution.Value = Distribution.LINEAR,
                       override val doc: String = "continuous param space",
                       seed: Int = 100) extends ParamSpace[Double](name, doc) {

  private val helper: String = "supported format of continuous parameter: [0,1] or [0,1,100]"

  def this(name: String, lower: Double, upper: Double) = {
    this(name, lower, upper, -1)
  }

  def this(name: String, config: String) = {
    this(name, 0, 1, -1)
    val items = parseConfig(config)
    lower = items._1
    upper = items._2
    num = items._3
    if (num != -1) {
      isGrid = true
      gridValues = toGrid
    }
  }

  def parseConfig(config: String): (Double, Double, Int) = {
    val ret: (Double, Double, Int) = config.trim match {
      case _ if config.contains(",") =>
        val splits = config.split(',')
        splits.length match {
          case 2 => (splits(0).toDouble, splits(1).toDouble, -1)
          case 3 => (splits(0).toDouble, splits(1).toDouble, splits(2).toInt)
        }
      case _ => throw new AutoMLException(s"invalid discrete, $helper")
    }
    ret
  }

  require(lower < upper, s"lower bound should less than upper bound")

  val rd = new Random(seed)

  var isGrid: Boolean = if (num == -1) false else true
  var gridValues: Array[Double] = if (isGrid) toGrid else Array.empty

  def toGrid(): Array[Double] = {
    var ret: ArrayBuffer[Double] = ArrayBuffer[Double]()
    distribution match {
      case Distribution.LINEAR =>
        val interval: Double = (upper - lower) / (num - 1)
        (0 until num).foreach { i =>
          ret += lower + i * interval
        }
      case _ => println(s"Distribution $distribution not supported")
    }

    ret.toArray
  }

  def getLower: Double = lower

  def getUpper: Double = upper

  def getValues: Array[Double] = gridValues

  def numValues: Int = num

  def toGridSearch: ParamSpace[Double] = this

  def toRandomSpace: ParamSpace[Double] = this

  override def sample(size: Int): Array[Double] = Array.fill[Double](size)(sampleOne)

  def sampleOne(): Double = {
    if (isGrid)
      gridValues(rd.nextInt(numValues))
    else
      lower + (upper - lower) * rd.nextDouble()
  }

  override def toString: String = s"ContinuousSpace[$name]: (${gridValues mkString(",")})"

}

object ContinuousSpace {

  def main(args: Array[String]): Unit = {
    val obj = new ContinuousSpace("test", "0,10")
    println(obj.toString)
    println(obj.sample(2).mkString(","))
  }
}