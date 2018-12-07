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


package com.tencent.angel.spark.ml.automl.tuner.config

import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import com.tencent.angel.spark.ml.automl.tuner.parameter.ParamSpace
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql.types._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class ConfigurationSpace(
                          val name: String,
                          var paramDict: Map[String, ParamSpace[Double]] = Map()) {

  val LOG: Log = LogFactory.getLog(classOf[ConfigurationSpace])

  var numParams: Int = paramDict.size

  var fields: ArrayBuffer[StructField] = new ArrayBuffer[StructField]()

  var param2Idx: Map[String, Int] = paramDict.keys.zipWithIndex.toMap
  var idx2Param: Map[Int, String] = param2Idx.map(_.swap)

  def getParamNum: Int = numParams

  def addParams(params: List[ParamSpace[Double]]): Unit = {
    params.foreach(addParam)
  }

  def addParam(param: ParamSpace[Double]): Unit = {
    if (!paramDict.contains(param.name)) {
      fields += DataTypes.createStructField(param.name, DataTypes.DoubleType, false)
      paramDict += (param.name -> param)
      param2Idx += (param.name -> numParams)
      idx2Param += (numParams -> param.name)
      numParams += 1
    }
    println(s"add param ${param.toString}, current params: ${paramDict.keySet.mkString(",")}")
  }

  def getFields: Array[StructField] = fields.toArray

  def getParams: List[ParamSpace[Double]] = paramDict.values.toList

  def getParamByName(name: String): Option[ParamSpace[Double]] = paramDict.get(name)

  def getIdxByParam(name: String): Option[Int] = param2Idx.get(name)

  def getParamByIdx(idx: Int): Option[ParamSpace[Double]] = paramDict.get(idx2Param.getOrElse(idx, "none"))

  // TODO: Store historical configurations to avoid redundancy.
  def sampleConfig(size: Int): List[Configuration] = {
    var configs: ListBuffer[Configuration] = new ListBuffer[Configuration]

    var missing: Int = 0
    do {
      missing = size - configs.length
      println(s"num of params: $numParams")
      var vectors: List[Vector] = List.fill(missing)(Vectors.dense(new Array[Double](numParams)))
      param2Idx.foreach { case (paramName, paramIdx) =>
        paramDict.get(paramName) match {
          case Some(param) =>
            param.sample(missing).zipWithIndex.foreach { case (f,i) =>
              vectors(i).toArray(paramIdx) = f
            }
          case None => LOG.info(s"Cannot find $paramName.")
        }
      }
      vectors.filter(validConfig).foreach{ vec =>
        configs += new Configuration(this, vec)
      }
    } while(configs.length < size)

    configs.toList
  }

  // TODO: Implement this func
  def validConfig(vec: Vector): Boolean = true
}