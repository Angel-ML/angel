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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm

import com.tencent.angel.client.{AngelClient, AngelClientFactory}
import com.tencent.angel.conf.{AngelConfiguration, MatrixConfiguration}
import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.algorithm.lda.{LDAModel, LDATrainTask}
import com.tencent.angel.ml.algorithm.lda.LDAModel._
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.protobuf.generated.MLProtos
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem}
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.log4j.PropertyConfigurator
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.mockito.runners.MockitoJUnitRunner

@RunWith(classOf[MockitoJUnitRunner])
class LDATest {

  private val LOG: Log = LogFactory.getLog(classOf[LDATest])
  private val LOCAL_FS: String = FileSystem.DEFAULT_FS
  private val TMP_PATH: String = System.getProperty("java.io.tmpdir", "/tmp")
  private var client: AngelClient = null
  PropertyConfigurator.configure("../log4j.properties")
  LOG.info(System.getProperty("user.dir"))

  @Before
  def setup(): Unit = {
    val inputPath: String = "./src/test/data/nips.train"

    // Set basic configuration keys
    val conf: Configuration = new Configuration
    conf.setBoolean("mapred.mapper.new-api", true)
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, classOf[LDATrainTask].getName)

    // Use local deploy mode
    conf.set(AngelConfiguration.ANGEL_DEPLOY_MODE, "LOCAL")

    // Set input and output path
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, classOf[CombineTextInputFormat].getName)
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, inputPath)
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out")

    // Set angel resource parameters #worker, #task, #PS
    conf.setInt(AngelConfiguration.ANGEL_WORKERGROUP_NUMBER, 1)
    conf.setInt(AngelConfiguration.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.setInt(AngelConfiguration.ANGEL_PS_NUMBER, 1)

    conf.set("angel.task.user.task.class", classOf[LDATrainTask].getName)

    client = AngelClientFactory.get(conf)

    // Set LDA parameters #V, #K
    val V = 12420
    val K = 1024

    conf.setInt(WORD_NUM, V)
    conf.setInt(TOPIC_NUM, K)
    conf.setInt(PARALLEL_NUM, 1)
    conf.setInt(MLConf.ML_EPOCH_NUM, 100)

//    val stale1 = "0"
//    val stale2 = "0"
//
//    conf.set(WORD_TOPIC_STALENESS, stale1)
//    conf.set(TOPIC_STALENESS, stale2)

    val lDAModel = new LDAModel(conf)

    // Model will be saved to HDFS, now set the path
    lDAModel.setSavePath(conf)

    // Load model meta to client
    client.loadModel(lDAModel)

  }

  @Test
  def run(): Unit = {

    client.submit()

    // Start
    client.start()

    // Run user task and wait for completion, user task is set in "angel.task.user.task.class"
    client.waitForCompletion()

    // Save the trained model to HDFS
//    client.saveModel(lDAModel)

    client.stop()
  }

}
