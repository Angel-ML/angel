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

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.classification.sparselr.{SparseLRRunner, SparseLRTrainTask}
import com.tencent.angel.ml.conf.MLConf
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
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, classOf[SparseLRTrainTask].getName)

    // Use local deploy mode
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL")

    // Set input data format
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[CombineTextInputFormat].getName)

    // Set angel resource parameters #worker, #task, #PS
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 100)

    // Set memory storage
    conf.set(AngelConf.ANGEL_TASK_DATA_STORAGE_LEVEL, "memory")

    conf.set("angel.task.user.task.class", classOf[SparseLRTrainTask].getName)
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true)

    // Feature number of train data
    val featureNum: Int = 124
    // Total iteration number
    val epochNum: Int = 5

    conf.setInt(ML_FEATURE_INDEX_RANGE, featureNum)
    conf.setInt(ML_EPOCH_NUM, epochNum)
    conf.setInt(ANGEL_WORKER_THREAD_NUM, 4)
    conf.set(ML_DATA_INPUT_FORMAT, "libsvm")

  }

  @Test
  def testOnLocalCluster(): Unit = {
    trainOnLocalClusterTest()
    predictTest()
  }

  @Test
  def trainOnLocalClusterTest(): Unit = {
    val inputPath: String = "./src/test/data/lr/a9a.train_label"

    // Set training data path
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath)
    // Set save model path
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/admmlrmodel")
    // Set action type train
    conf.set(AngelConf.ANGEL_ACTION_TYPE, ANGEL_ML_TRAIN)

    val runner = new SparseLRRunner
    runner.train(conf)
  }

  @Test
  def predictTest() {
    val inputPath: String = "./src/test/data/lr/a9a.train_label"
    val loadPath: String = LOCAL_FS + TMP_PATH + "/admmlrmodel"
    val predictPath: String = LOCAL_FS + TMP_PATH + "/ADMMLRpredict"

    // Set trainning data path
    conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, inputPath)
    // Set load model path
    conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, loadPath)
    // Set predict result path
    conf.set(AngelConf.ANGEL_PREDICT_PATH, predictPath)
    // Set actionType prediction
    conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_PREDICT)

    val runner = new SparseLRRunner
    runner.predict(conf)
  }

}
