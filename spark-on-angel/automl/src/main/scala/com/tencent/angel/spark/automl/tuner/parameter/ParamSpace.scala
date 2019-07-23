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


/**
  * Base class of a single parameter's search space.
  *
  * @param name : Name of the parameter
  */
abstract class ParamSpace[+T: ClassTag](val name: String,
                                        val doc: String = "param with search space") {

  val pType: String

  val vType: String

  def sample(size: Int): List[T]

  def sampleOne(): T

  def getValues: Array[Double]

  def numValues: Int
}

object ParamSpace {

  def fromConfigString(name: String, config: String): ParamSpace[Double] = {
    val vType =
      if (config.trim.startsWith("[") && config.trim.endsWith("]"))
        "continuous"
      else if (config.trim.startsWith("{") && config.trim.endsWith("}"))
        "discrete"
      else "none"
    vType match {
      case "continuous" => ContinuousSpace(name, config)
      case "discrete" => DiscreteSpace[Double](name, config)
      case _ => throw new AutoMLException(s"auto param config is not supported")
    }
  }

}
