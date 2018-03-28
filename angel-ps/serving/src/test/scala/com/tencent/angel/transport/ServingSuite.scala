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

import java.net.InetSocketAddress

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.serving.client.ServingClient
import com.tencent.angel.serving.ml.lr.LRServingModel
import com.tencent.angel.serving.{PredictData, Util}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.scalatest.{BeforeAndAfterAll, FunSuite, Outcome}

class ServingSuite extends FunSuite with BeforeAndAfterAll {
  private[this] var _conf: Configuration = new Configuration()

  def conf: Configuration = _conf

  private val LOCAL_FS = "file:///"
  private val TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp")

  def conf_=(value: Configuration): Unit = {
    _conf = value
  }

  {
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL")
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 3)
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true)
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true)
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[CombineTextInputFormat].getName)
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out")
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in")
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log")
    conf.setBoolean(AngelConf.ANGEL_PLUGIN_SERVICE_ENABLE, true)
    conf.set("angel.serving.model.lr.class", classOf[LRServingModel].getName)
    conf.setInt("angel.serving.lr.sharding.num", 3)
  }

  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    try {
      println(s"\n\n===== TEST OUTPUT FOR $suiteName: '$testName' ======\n")
      test()
    } finally {
      println(s"\n===== FINISHED $suiteName: '$testName' ======\n")
    }
  }

  test("lr serving") {
    val client = AngelClientFactory.get(conf)
    client.startPSServer()

    val masterIP = client.getMasterLocation.getIp
    val masterPort = client.getMasterLocation.getPort
    val masterAddr = new InetSocketAddress(masterIP,masterPort)
    val servingClient = ServingClient.create(masterAddr, conf)

    servingClient.loadModel("lr", "../data/exampledata/ServingData/lr_model", 3, 3, "")
    var stop: Boolean = false
    while (!stop) {
      if (servingClient.isServable("lr")) {
        stop = true
      }
      Thread.sleep(50)
    }
    val iterator = Util.read(conf, new Path("../data/exampledata/ServingData/lr_model/a9a.train"), 124, "libsvm")
    var i = 0
    while (iterator.hasNext) {
      println(servingClient.getModel("lr").get.getCoordinator().predict(new PredictData[TVector](iterator.next())))
      i = i + 1

      if (i % 100 == 0) println(i)
    }
  }
}
