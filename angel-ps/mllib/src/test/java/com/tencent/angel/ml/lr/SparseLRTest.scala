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

package com.tencent.angel.ml.lr

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.classification.sparselr.{SparseLogisticRegressionRunner, SparseLogisticRegressionTask}
import com.tencent.angel.ml.conf.MLConf._
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.log4j.PropertyConfigurator
import org.junit.runner.RunWith
import org.junit.{Before, Test}
import org.mockito.runners.MockitoJUnitRunner


@RunWith(classOf[MockitoJUnitRunner])
class SparseLRTest {

  private val LOG: Log = LogFactory.getLog(classOf[SparseLRTest])
  private val LOCAL_FS: String = FileSystem.DEFAULT_FS
  private val TMP_PATH: String = System.getProperty("java.io.tmpdir", "/tmp")
  private val conf: Configuration = new Configuration
  PropertyConfigurator.configure("../conf/log4j.properties")
  LOG.info(System.getProperty("user.dir"))

  @Before
  def setup(): Unit = {
    // Set basic configuration keys
    conf.setBoolean("mapred.mapper.new-api", true)
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, classOf[SparseLogisticRegressionTask].getName)

    // Use local deploy mode
    conf.set(AngelConfiguration.ANGEL_DEPLOY_MODE, "LOCAL")

    // Set input data format
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, classOf[CombineTextInputFormat].getName)

    // Set angel resource parameters #worker, #task, #PS
    conf.setInt(AngelConfiguration.ANGEL_WORKERGROUP_NUMBER, 1)
    conf.setInt(AngelConfiguration.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.setInt(AngelConfiguration.ANGEL_PS_NUMBER, 1)

    // Set memory storage
    conf.set(AngelConfiguration.ANGEL_TASK_DATA_STORAGE_LEVEL, "memory")

    conf.set("angel.task.user.task.class", classOf[SparseLogisticRegressionTask].getName)
    conf.setBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true)

    // Feature number of train data
    val featureNum: Int = 124
    // Total iteration number
    val epochNum: Int = 10

    conf.setInt(ML_FEATURE_NUM, featureNum)
    conf.setInt(ML_EPOCH_NUM, epochNum)
    conf.setInt(ML_WORKER_THREAD_NUM, 4)
  }

  @Test
  def trainOnLocalClusterTest(): Unit = {
    val inputPath: String = "./src/test/data/lr/a9a.train_label";
    val logPath: String = "./src/test/log"

    // Set training data path
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, inputPath)
    // Set save model path
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model")
    // Set log path
    conf.set(AngelConfiguration.ANGEL_LOG_PATH, logPath)
    // Set action type train
    conf.set(AngelConfiguration.ANGEL_ACTION_TYPE, ANGEL_ML_TRAIN)

    conf.set(ML_DATAFORMAT, "libsvm")

    val runner = new SparseLogisticRegressionRunner
    runner.train(conf)
  }

}
