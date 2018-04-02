
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

package com.tencent.angel.ml.GBDT

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLRunner
import com.tencent.angel.ml.conf.MLConf
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

class GBDTRunner extends MLRunner {

  val LOG = LogFactory.getLog(classOf[GBDTRunner])

  var featureNum: Int = 0
  var featureNonzero: Int = 0
  var maxTreeNum: Int = 0
  var maxTreeDepth: Int = 0
  var splitNum: Int = 0
  var featureSampleRatio: Float = 0.0f

  override def train(conf: Configuration): Unit = {
    var indexRange = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
    val psNumber = conf.getInt(AngelConf.ANGEL_PS_NUMBER, 1)

    if (indexRange % psNumber != 0) {
      indexRange = (indexRange / psNumber + 1) * psNumber
      conf.setLong(MLConf.ML_FEATURE_INDEX_RANGE, indexRange)
      LOG.info(s"PS num: ${psNumber}, true feat num: ${indexRange}")
    }

    conf.setLong(MLConf.ML_FEATURE_INDEX_RANGE, indexRange)

    train(conf, GBDTModel(conf), classOf[GBDTTrainTask])
  }

  override def predict(conf: Configuration) {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    super.predict(conf, GBDTModel(conf), classOf[GBDTPredictTask])
  }

  /**
    * Incremental training job to obtain a model based on a trained model
    */
  override def incTrain(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    val path = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH)
    if (path == null) throw new AngelException("parameter '" + AngelConf.ANGEL_LOAD_MODEL_PATH + "' should be set to load model")
    train(conf)
  }
}
