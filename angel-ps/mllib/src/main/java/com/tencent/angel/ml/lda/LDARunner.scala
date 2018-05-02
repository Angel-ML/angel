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

package com.tencent.angel.ml.lda

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.data.inputformat.BalanceInputFormat
import com.tencent.angel.ml.MLRunner
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

class LDARunner extends MLRunner {

  val LOG = LogFactory.getLog(classOf[LDARunner])

  /**
    * Training job to obtain a model
    */
  override def train(conf: Configuration): Unit = {
    conf.setInt(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS, 1)
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[BalanceInputFormat].getName)

//    var mem = conf.getInt(AngelConf.ANGEL_WORKER_MEMORY_MB, -1)
//    if (mem == -1)
//      mem = conf.getInt(AngelConf.ANGEL_WORKER_MEMORY_GB, 1) * 1000
//    var javaOpts = s"-Xmx${mem}M -Xms${mem}M -XX:+UseConcMarkSweepGC -XX:+PrintGCTimeStamps -XX:+PrintGCDetails"
//    LOG.info(javaOpts)
//    conf.set(AngelConf.ANGEL_WORKER_JAVA_OPTS, javaOpts)
//
//    mem = conf.getInt(AngelConf.ANGEL_PS_MEMORY_MB, -1)
//    if (mem == -1)
//      mem = conf.getInt(AngelConf.ANGEL_PS_MEMORY_GB, 1) * 1000
//    javaOpts = s"-Xmx${mem}M -Xms${mem}M -XX:+UseConcMarkSweepGC -XX:+PrintGCTimeStamps -XX:+PrintGCDetails"
//    conf.set(AngelConf.ANGEL_PS_JAVA_OPTS, javaOpts)
//    LOG.info(javaOpts)


    LOG.info(s"n_tasks=${conf.getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 0)}")
    //    train(conf, new LDAModel(conf), classOf[LDATrainTask])

    LOG.info(s"save path=${conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH)}")

    if (conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH) == null)
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, conf.get(LDAModel.SAVE_PATH))

    LOG.info(s"save path=${conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH)}")

    val client = AngelClientFactory.get(conf)

    client.startPSServer()
    client.loadModel(new LDAModel(conf))
    client.runTask(classOf[LDATrainTask])
    client.waitForCompletion()
    //    client.saveModel(model)

    client.stop()
  }

  /**
    * Using a model to predict with unobserved samples
    */
  override def predict(conf: Configuration): Unit = {
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[BalanceInputFormat].getName)


    var mem = conf.getInt(AngelConf.ANGEL_WORKER_MEMORY_MB, -1)
    if (mem == -1)
      mem = conf.getInt(AngelConf.ANGEL_WORKER_MEMORY_GB, 1) * 1000
    var javaOpts = s"-Xmx${mem}M -Xms${mem}M -XX:+UseConcMarkSweepGC -XX:+PrintGCTimeStamps -XX:+PrintGCDetails"
    LOG.info(javaOpts)
    conf.set(AngelConf.ANGEL_WORKER_JAVA_OPTS, javaOpts)

    mem = conf.getInt(AngelConf.ANGEL_PS_MEMORY_MB, -1)
    if (mem == -1)
      mem = conf.getInt(AngelConf.ANGEL_PS_MEMORY_GB, 1) * 1000
    javaOpts = s"-Xmx${mem}M -Xms${mem}M -XX:+UseConcMarkSweepGC -XX:+PrintGCTimeStamps -XX:+PrintGCDetails"
    conf.set(AngelConf.ANGEL_PS_JAVA_OPTS, javaOpts)
    LOG.info(javaOpts)
    val client = AngelClientFactory.get(conf)

    client.startPSServer()
    client.loadModel(new LDAModel(conf))
    client.runTask(classOf[LDAPredictTask])
    client.waitForCompletion()
    //    client.saveModel(model)

    client.stop()
  }

  /**
    * Incremental training job to obtain a model based on a trained model
    */
  override def incTrain(conf: Configuration): Unit = ???
}
