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
 *
 */

package com.tencent.angel.ml.LDA

import com.tencent.angel.client.{AngelClient, AngelClientFactory}
import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.lda.LDAModel._
import com.tencent.angel.ml.lda.{LDAModel, LDATrainTask}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.log4j.PropertyConfigurator
import org.junit.runner.RunWith
import org.junit.{Before, Test}
import org.mockito.runners.MockitoJUnitRunner

@RunWith(classOf[MockitoJUnitRunner])
class LDATest {
  private val conf: Configuration = new Configuration
  private val LOG: Log = LogFactory.getLog(classOf[LDATest])
  private val LOCAL_FS: String = FileSystem.DEFAULT_FS
  private val TMP_PATH: String = System.getProperty("java.io.tmpdir", "/tmp")
  private var client: AngelClient = null
  PropertyConfigurator.configure("../conf/log4j.properties")
  LOG.info(System.getProperty("user.dir"))

  @Before
  def setup(): Unit = {
    val inputPath: String = "./src/test/data/LDA/nips.train"

    // Set basic configuration keys
    conf.setBoolean("mapred.mapper.new-api", true)
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, classOf[LDATrainTask].getName)

    // Use local deploy mode
    conf.set(AngelConfiguration.ANGEL_DEPLOY_MODE, "LOCAL")

    // Set input and output path
    conf.setBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true)
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, classOf[CombineTextInputFormat].getName)
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, inputPath)
    conf.set(AngelConfiguration.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/LOG/ldalog")
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out")

    // Set angel resource parameters #worker, #task, #PS
    conf.setInt(AngelConfiguration.ANGEL_WORKERGROUP_NUMBER, 1)
    conf.setInt(AngelConfiguration.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.setInt(AngelConfiguration.ANGEL_PS_NUMBER, 1)

    conf.set("angel.task.user.task.class", classOf[LDATrainTask].getName)

    client = AngelClientFactory.get(conf)

    // Set LDA parameters #V, #K
    val V = 12420
    val K = 100

    conf.setInt(WORD_NUM, V)
    conf.setInt(TOPIC_NUM, K)
    conf.setInt(MLConf.ML_WORKER_THREAD_NUM, 1)
    conf.setInt(MLConf.ML_EPOCH_NUM, 20)
  }

  @Test
  def run(): Unit = {
    //start PS
    client.startPSServer()

    //init model
    val lDAModel = new LDAModel(conf)

    // Load model meta to client
    client.loadModel(lDAModel)

    // Start
    client.runTask(classOf[LDATrainTask])

    // Run user task and wait for completion, user task is set in "angel.task.user.task.class"
    client.waitForCompletion()

    // Save the trained model to HDFS
    //    client.saveModel(lDAModel)

    client.stop()
  }

}
