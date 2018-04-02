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
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.lda.LDAModel._
import com.tencent.angel.ml.lda.{LDAModel, LDAPredictTask, LDATrainTask}
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
    val inputPath: String = "./src/test/data/LDA/nips.doc"

    // Set basic configuration keys
    conf.setBoolean("mapred.mapper.new-api", true)
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, classOf[LDATrainTask].getName)

    // Use local deploy mode
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL")
    conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 100)

    // Set input and output path
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true)
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[CombineTextInputFormat].getName)
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath)
    conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, inputPath)
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/LOG/ldalog")

    conf.setInt(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS, 1)

    // Set angel resource parameters #worker, #task, #PS
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1)

    client = AngelClientFactory.get(conf)

    // Set LDA parameters #V, #K
    val V = 12420
    val K = 1000

    conf.setInt(WORD_NUM, V)
    conf.setInt(TOPIC_NUM, K)
    conf.setInt(MLConf.ANGEL_WORKER_THREAD_NUM, 1)
    conf.setInt(MLConf.ML_EPOCH_NUM, 10)
    conf.setBoolean(SAVE_DOC_TOPIC, false)
    conf.setBoolean(SAVE_WORD_TOPIC, true)
  }

  @Test
  def testTrainAndPredict(): Unit = {
    train()
    inference()
  }

  def train(): Unit = {

    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out")
    LOG.info(conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH))
    //start PS
    client.startPSServer()

    //init model
    val model = new LDAModel(conf)

    // Load model meta to client
    client.loadModel(model)

    // Start
    client.runTask(classOf[LDATrainTask])

    // Run user task and wait for completion, user task is set in "angel.task.user.task.class"
    client.waitForCompletion()

    // Save the trained model to HDFS
    //    client.saveModel(lDAModel)

    client.stop()
  }

  def inference(): Unit = {
    conf.set(AngelConf.ANGEL_ACTION_TYPE, "predict")

    conf.set(AngelConf.ANGEL_PREDICT_PATH, LOCAL_FS + TMP_PATH + "/out_1")
    conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out")


    client.startPSServer()
    val model = new LDAModel(conf)
    client.loadModel(model)
    client.runTask(classOf[LDAPredictTask])
    client.waitForCompletion()
    client.stop()

  }
}
