/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.jarvis

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration

import com.tencent.angel.jarvis.utils.{AngelParam, ArgsParser, TeslaParam}
import com.tencent.angel.jarvis.utils.TeslaParam.ActionType
import com.tencent.angel.utils.AngelRunJar

object Submitter {

  def main(args: Array[String]): Unit = {

    val jobConf: Configuration = new Configuration(false)
    val confMap = ArgsParser.parse(args)

    // optimize param to Angel
    val angelConfMap = convert2AngelKey(confMap)
    for (entry <- angelConfMap) {
      jobConf.set(entry._1, entry._2)
    }

    try {
      AngelRunJar.submit(jobConf)
    } catch {
      case x: Exception =>
        println("submit job failed ", x)
        System.exit(-1)
    }
  }

  def convert2AngelKey(confMap: mutable.HashMap[String, String]): Map[String, String] = {
    // check the action type
    val actionType = confMap.get(TeslaParam.ACTION_TYPE)
    val modelPath = confMap.get(TeslaParam.MODEL_PATH)

    if (actionType.isDefined) confMap.put(AngelParam.ANGEL_ACTION_TYPE, actionType.get)
    if (modelPath.isDefined) {
      if (actionType.contains(ActionType.TRAIN)) {
        confMap.put(AngelParam.ANGEL_SAVE_MODEL_PATH, actionType.get)
      } else if (actionType.contains(ActionType.PREDICT)) {
        confMap.put(AngelParam.ANGEL_LOAD_MODEL_PATH, actionType.get)
      } else {
        println("[WARNING] actionType must be set to train or predict")
      }
    }
    confMap.toMap
  }

}
