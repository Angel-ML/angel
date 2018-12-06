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

  override def sample(size: Int): List[T] = List.fill(size)(sample)

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
