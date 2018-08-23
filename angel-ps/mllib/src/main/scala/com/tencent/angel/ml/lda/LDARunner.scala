/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
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
import com.tencent.angel.ml.core.MLRunner
import com.tencent.angel.ml.core.conf.MLConf
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

class LDARunner extends MLRunner {

  val LOG = LogFactory.getLog(classOf[LDARunner])

  def setConfs(conf: Configuration): Unit = {
    conf.setInt(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS, 1)
    // Number of tasks per worker is 1
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    // Input format
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[BalanceInputFormat].getName)

    val numTopics = conf.getInt(LDAModel.TOPIC_NUM, 10)
    val numWorkers = conf.getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 10)
    val numServers = conf.getInt(AngelConf.ANGEL_PS_NUMBER, 10)
    val numThreads = conf.getInt(MLConf.ANGEL_WORKER_THREAD_NUM, 2)
    val alpha = conf.getFloat(LDAModel.ALPHA, 50.0F / numTopics)
    val beta = conf.getFloat(LDAModel.BETA, 0.01F)

    LOG.info(s"numTopics=$numTopics" +
      s" numWorkers=$numWorkers" +
      s" numPs=$numServers" +
      s" numThreads=$numThreads" +
      s" alpha=$alpha" +
      s" beta=$beta")
  }

  def setJVMOpts(conf: Configuration): Unit = {

    var totalMemory = conf.getInt(AngelConf.ANGEL_WORKER_MEMORY_MB, -1)
    if (totalMemory == -1)
      totalMemory = conf.getInt(AngelConf.ANGEL_WORKER_MEMORY_GB, 1) * 1000

    var heapMemoryFraction = conf.getDouble("angel.jvm.heap.fraction", 0.8)
    var directMemoryFraction = 1 - heapMemoryFraction

    var heapSize = (totalMemory * heapMemoryFraction).toInt
    var directSize = (totalMemory * directMemoryFraction).toInt

    var javaOpts = s"-Xmx${heapSize}M " +
      s"-XX:+UseConcMarkSweepGC " +
      s"-XX:+PrintGCTimeStamps " +
      s"-XX:+PrintGCDetails " +
      s"-XX:MaxDirectMemorySize=${directSize}M"

    LOG.info(s"worker JVM settings: $javaOpts")
    conf.set(AngelConf.ANGEL_WORKER_JAVA_OPTS, javaOpts)

    totalMemory = conf.getInt(AngelConf.ANGEL_PS_MEMORY_MB, -1)
    if (totalMemory == -1)
      totalMemory = conf.getInt(AngelConf.ANGEL_PS_MEMORY_GB, 1) * 1000

    directMemoryFraction = math.min(0.5, directMemoryFraction * 2)
    heapMemoryFraction = 1 - directMemoryFraction
    heapSize = (totalMemory * heapMemoryFraction).toInt
    directSize = (totalMemory * directMemoryFraction).toInt
    javaOpts = s"-Xmx${heapSize}M " +
      s"-XX:+UseConcMarkSweepGC " +
      s"-XX:+PrintGCTimeStamps " +
      s"-XX:+PrintGCDetails " +
      s"-XX:MaxDirectMemorySize=${directSize}M"

    conf.set(AngelConf.ANGEL_PS_JAVA_OPTS, javaOpts)
    LOG.info(s"ps JVM settings: $javaOpts")
  }

  /**
    * Training job to obtain a model
    */
  override def train(conf: Configuration): Unit = {

    if (conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH) == null)
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, conf.get(LDAModel.SAVE_PATH))

    setConfs(conf)
    setJVMOpts(conf)

    val client = AngelClientFactory.get(conf)

    client.startPSServer()
    client.loadModel(new LDAModel(conf))
    client.runTask(classOf[LDATrainTask])
    client.waitForCompletion()
    client.stop()
  }

  /**
    * Using a model to predict with unobserved samples
    */
  override def predict(conf: Configuration): Unit = {

    setConfs(conf)
    setJVMOpts(conf)

    val client = AngelClientFactory.get(conf)

    client.startPSServer()
    client.loadModel(new LDAModel(conf))
    client.runTask(classOf[LDAPredictTask])
    client.waitForCompletion()
    client.stop()
  }
}
