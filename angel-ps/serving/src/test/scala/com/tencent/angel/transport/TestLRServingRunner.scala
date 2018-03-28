/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/BSD-3-Clause
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package com.tencent.angel.transport

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.serving.client.ServingClient
import com.tencent.angel.serving.ml.lr.LRServingModel
import com.tencent.angel.serving.{PredictData, Util}
import com.tencent.angel.utils.ConfUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat

class TestLRServingRunner {

  private def getConf: Configuration = {
    val conf = ConfUtils.initConf(Array[String]())

    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true)
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true)
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[CombineTextInputFormat].getName)
    conf.setBoolean(AngelConf.ANGEL_PLUGIN_SERVICE_ENABLE, true)

    conf.set(s"angel.serving.model.lr.class", classOf[LRServingModel].getName)
    conf.setInt(s"angel.serving.lr.sharding.num", 3)

    conf
  }

  def main(args: Array[String]): Unit = {
    val conf = getConf
    val client = AngelClientFactory.get(conf)
    client.startPSServer()
    val servingClient = ServingClient.create(client)

    val dim = 124
    val predictDataPath = new Path("../data/exampledata/ServingData/lr_model/a9a.train")
    val iterator = Util.read(conf, predictDataPath, dim)
    var i = 0
    while (iterator.hasNext) {
      println(servingClient.getModel("lr").get.getCoordinator().predict(new PredictData[TVector](iterator.next())))
      i = i + 1

      if (i % 100 == 0) println(i)
    }
  }
}
