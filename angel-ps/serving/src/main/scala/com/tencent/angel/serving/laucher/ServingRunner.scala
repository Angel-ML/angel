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

package com.tencent.angel.serving.laucher

import com.tencent.angel.{AppSubmitter, RunningMode}
import com.tencent.angel.client.{AngelClient, AngelClientFactory}
import com.tencent.angel.conf.AngelConf
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat

class ServingRunner extends AppSubmitter {
  private val LOG: Log = LogFactory.getLog(classOf[ServingRunner])
  private var client: AngelClient = _
  private var stop: Boolean = false

  def setConf(conf: Configuration): Unit = {
    val dummySpliter = conf.get(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER)
    if (dummySpliter == null || dummySpliter.isEmpty) {
      conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true)
    }

    val jobOpDeleteOnExist = conf.get(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST)
    if (jobOpDeleteOnExist== null || jobOpDeleteOnExist.isEmpty) {
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true)
    }

    val inputFormatClass = conf.get(AngelConf.ANGEL_INPUTFORMAT_CLASS)
    if (inputFormatClass == null || inputFormatClass.isEmpty) {
      conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[CombineTextInputFormat].getName)
    }

    conf.setBoolean(AngelConf.ANGEL_PLUGIN_SERVICE_ENABLE, true)
    conf.set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString)
  }

  def startServer(conf: Configuration): Unit = {
    client = AngelClientFactory.get(conf)
    LOG.info("Start PS server for distribute serving ...")
    client.startPSServer()
    LOG.info("All the PS server started !")

    val masterLocation = client.getMasterLocation

    println()
    println(s"\tMaster Location IP:\t\t${masterLocation.getIp}")
    println(s"\tMaster Location Port:\t\t${masterLocation.getPort}")
    println()

    while (!stop) {
      Thread.sleep(500)
    }
  }

  def stopServer(): Unit = {
    client.stop()
    stop = true
  }

  /**
    * Submit application on Angel
    *
    * @param conf the conf
    * @throws Exception exception
    */
  @throws[Exception]
  override
  def submit(conf: Configuration): Unit = {
    setConf(conf)
    startServer(conf)
  }
}
