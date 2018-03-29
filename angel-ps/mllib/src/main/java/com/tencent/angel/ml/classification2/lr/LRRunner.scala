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

package com.tencent.angel.ml.classification2.lr

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLRunner
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

/**
  * Run logistic regression task on angel
  */

class LRRunner extends MLRunner {
  private val LOG = LogFactory.getLog(classOf[LRRunner])

  /**
    * Run LR train task
    *
    * @param conf : configuration of algorithm and resource
    */
  override
  def train(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    train(conf, LRModel(conf), classOf[LRTrainTask])
  }

  /*
   * Run LR predict task
   * @param conf: configuration of algorithm and resource
   */
  override
  def predict(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    super.predict(conf, LRModel(conf), classOf[LRPredictTask])
  }

  /*
   * Run LR incremental train task
   * @param conf: configuration of algorithm and resource
   */
  def incTrain(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    val path = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH)
    if (path == null) throw new AngelException("parameter '" + AngelConf.ANGEL_LOAD_MODEL_PATH + "' should be set to load model")
    train(conf, LRModel(conf), classOf[LRTrainTask])
  }
}
