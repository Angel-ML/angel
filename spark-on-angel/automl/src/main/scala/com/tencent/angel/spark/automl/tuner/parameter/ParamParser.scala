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

import scala.beans.BeanProperty

/**
  * parse configuration of auto tuning from the command
  * valid format: PARAM_NAME|PARAM_TYPE|VALUE_TYPE|PARAM_RANGE|OPTIONS, multiple params are separated by #
  * example: ml.learn.rate|C|double|0.01,1|linear#ml.learn.decay|D|double|0,0.01,0.1
  */
object ParamParser {

  val helper = "supported format: PARAM_NAME|PARAM_TYPE|VALUE_TYPE|PARAM_RANGE|OPTIONS, OPTIONS is optional"
  val helper_param_type = "param type should be D, C or CA (D means discrete, C means continuous, and CA means categorical"
  val helper_value_type = "value type should be float, double, int or long"

  val INTER_PARAM_SEP = "#"
  val INNER_PARAM_SEP = "\\|"

  def parse(input: String): Array[ParamConfig] = {
    separateParams(input).map(parseOneParam)
  }

  /**
    * separate the config command to a set of parameter config
    */
  def separateParams(input: String): Array[String] = {
    val params = input.split(INTER_PARAM_SEP)
    assert(params.nonEmpty, helper)
    params
  }

  /**
    * parse config for each parameter
    */
  def parseOneParam(input: String): ParamConfig = {
    val configs = input.split(INNER_PARAM_SEP)
    println(s"configs: ${configs.mkString(",")}")
    assert(configs.size == 4 || configs.size == 5, helper)
    val paramName = getParamName(configs)
    val paramType = getParamType(configs)
    val valueType = getValueType(configs, paramType)
    val paramRange = getParamRange(configs, paramType)
    val options = getOptions(configs)
    new ParamConfig(paramName, paramType, valueType, paramRange, options)
  }

  def getParamName(configs: Array[String]): String = configs(0)

  def getParamType(configs: Array[String]): String = {
    val paramType = configs(1).toUpperCase
    paramType match {
      case "D" => "discrete"
      case "C" => "continuous"
      case "CA" => "categorical"
      case _ => throw new AutoMLException(helper_param_type)
    }
  }

  def getValueType(configs: Array[String], paramType: String): String = {
    val valueType = configs(2).toLowerCase
    paramType match {
      case "discrete" =>
        assert(Array("float", "double", "int", "long").contains(valueType), helper_value_type)
        valueType
      case "continuous" =>
        "double"
      case "categorical" =>
        valueType
    }
  }

  def getParamRange(configs: Array[String], paramType: String): String = {
    paramType match {
      case "discrete" => configs(3).mkString("{","","}")
      case "continuous" => configs(3).mkString("[","","]")
      // TODO: use categorical specific format
      case "categorical" => configs(3)
    }
  }


  def getOptions(configs: Array[String]): Option[String] = {
    if (configs.size == 4)
      None
    else
      Some(configs(4))
  }

}

class ParamConfig(@BeanProperty var paramName: String,
                  @BeanProperty var paramType: String,
                  @BeanProperty var valueType: String,
                  @BeanProperty var paramRange: String,
                  @BeanProperty var option: Option[String])
