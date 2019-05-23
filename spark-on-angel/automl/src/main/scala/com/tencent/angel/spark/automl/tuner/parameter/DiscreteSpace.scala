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


package com.tencent.angel.spark.automl.tuner.parameter

import com.tencent.angel.spark.automl.utils.AutoMLException

import scala.reflect.ClassTag
import scala.util.Random

import scala.reflect._

/**
  * Search space with discrete values
  *
  * @param name   : Name of the parameter
  * @param values : List of all possible values
  */
class DiscreteSpace[T <: AnyVal : ClassTag](
                                             override val name: String,
                                             var values: Array[T],
                                             override val doc: String = "discrete param") extends ParamSpace[T](name, doc) {

  private val helper: String = "supported format of discrete parameter: {0.1,0.2,0.3,0.4} or {0.1:1:0.1}"

  override val pType: String = "discrete"
  override val vType = classTag[T].runtimeClass.getSimpleName.toLowerCase

  def this(name: String, config: String, doc: String) = {
    this(name, Array.empty[T], doc)
    this.values = parseConfig(config)
  }

  def this(name: String, config: String) = {
    this(name, config, "discrete param")
  }

  def parseConfig(input: String): Array[T] = {
    assert(input.startsWith("{") && input.endsWith("}"))
    val config = input.substring(1, input.length - 1)
    val values: Array[T] = config.trim match {
      case _ if config.contains(",") =>
        config.split(',').map(asType)
      case _ if config.contains(":") =>
        val splits = config.split(':')
        splits.length match {
          case 2 => (splits(0).toDouble to splits(1).toDouble by 1.0f).toArray.map(asType)
          case 3 => (splits(0).toDouble to splits(1).toDouble by splits(2).toDouble).toArray.map(asType)
          case _ => throw new AutoMLException(s"invalid discrete, $helper")
        }
      case _ => throw new AutoMLException(s"invalid discrete, $helper")
    }
    values
  }

  def asType(s: String): T = {
    val c = implicitly[ClassTag[T]].runtimeClass
    c match {
      case _ if c == classOf[Int] => s.toInt.asInstanceOf[T]
      case _ if c == classOf[Long] => s.toLong.asInstanceOf[T]
      case _ if c == classOf[Float] => s.toFloat.asInstanceOf[T]
      case _ if c == classOf[Double] => s.toDouble.asInstanceOf[T]
      case _ => throw new AutoMLException(s"auto param with type ${c} is not supported")
    }
  }

  def asType(s: Double): T = {
    val c = implicitly[ClassTag[T]].runtimeClass
    c match {
      case _ if c == classOf[Int] => s.toInt.asInstanceOf[T]
      case _ if c == classOf[Long] => s.toLong.asInstanceOf[T]
      case _ if c == classOf[Float] => s.toFloat.asInstanceOf[T]
      case _ if c == classOf[Double] => s.toDouble.asInstanceOf[T]
      case _ => throw new AutoMLException(s"auto param with type ${c} is not supported")
    }
  }

  def asDouble(num: AnyVal): Double = {
    num match {
      case i: Int => i.toDouble
      case i: Long => i.toLong
      case i: Float => i.toDouble
      case i: Double => i
      case _ => throw new AutoMLException(s"type ${num.getClass} is not supported")
    }
  }

  val rd = new Random()

  def getValues: Array[Double] = values.map(asDouble)

  def numValues: Int = values.length

  def toGridSearch: ParamSpace[T] = this

  def toRandomSpace: ParamSpace[T] = this

  def sample(size: Int): List[T] = {
    List.fill[T](size)(sampleOne)
  }

  def sampleOne(): T = values(rd.nextInt(numValues))

  override def toString: String = s"DiscreteSpace[$name]: (${values mkString (",")})"

}

object DiscreteSpace {

  def apply[T <: AnyVal : ClassTag](name: String, config: String): DiscreteSpace[T] = {
    new DiscreteSpace[T](name, config)
  }

  def main(args: Array[String]): Unit = {
    val obj = new DiscreteSpace[Int]("test", "1:10:1")
    println(obj.toString)
    println(obj.getValues(1))
    println(obj.sample(2).toString())
  }
}
