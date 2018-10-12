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


package com.tencent.angel.ml.auto.config

import com.tencent.angel.ml.auto.parameter.ParamSpace
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage
import com.tencent.angel.ml.math2.vector.IntFloatVector
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.ListBuffer

class ConfigurationSpace(val name: String, var paramDict: Map[String, ParamSpace[Float]] = Map()) {
  val LOG: Log = LogFactory.getLog(classOf[ConfigurationSpace])

  var paramNum: Int = paramDict.size
  var param2Idx: Map[String, Int] = paramDict.keys.zipWithIndex.toMap
  var idx2Param: Map[Int, String] = param2Idx.map(_.swap)

  def getParamNum: Int = paramNum

  def addParams(params: List[ParamSpace[Float]]): Unit = {
    params.foreach(addParam)
  }

  def addParam(param: ParamSpace[Float]): Unit = {
    if (!paramDict.contains(param.name)) {
      paramDict += (param.name -> param)
      paramNum += 1
    }
  }

  def getParams: List[ParamSpace[Float]] = paramDict.values.toList

  def getParamByName(name: String): Option[ParamSpace[Float]] = paramDict.get(name)

  def getIdxByParam(name: String): Option[Int] = param2Idx.get(name)

  def getParamByIdx(idx: Int): Option[ParamSpace[Float]] = paramDict.get(idx2Param.getOrElse(idx, "none"))

  // TODO: Store historical configurations to avoid redundancy.
  def sampleConfig(size: Int): List[Configuration] = {
    var configs: ListBuffer[Configuration] = new ListBuffer[Configuration]

    var missing: Int = 0
    do {
      missing = size - configs.length
      val vectors: List[IntFloatVector] = List.fill(missing)(new IntFloatVector(paramNum, new IntFloatDenseVectorStorage(paramNum)))
      param2Idx.foreach { case (paramName, paramIdx) =>
        paramDict.get(paramName) match {
          case Some(param) =>
            param.sample(missing).zipWithIndex.foreach { case (f,i) =>
                vectors(i).set(paramIdx, f)
            }
            vectors.filter(validConfig).foreach{ vec =>
              configs += new Configuration(this, vec)
            }
          case None => LOG.info(s"Cannot find $paramName.")
        }
      }
    } while(configs.length < size)

    configs.toList
  }

  // TODO: Implement this func
  def validConfig(vec: IntFloatVector): Boolean = true
}
