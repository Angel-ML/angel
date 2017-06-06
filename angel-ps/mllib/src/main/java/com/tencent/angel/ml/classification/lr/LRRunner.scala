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

package com.tencent.angel.ml.classification.lr


import com.tencent.angel.ml.MLRunner
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.MLRunner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.utils.HDFSUtils
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader

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
    conf.setInt("angel.worker.matrixtransfer.request.timeout.ms", 60000)

    var maxdimPathStr = conf.get(MLConf.ML_MAXDIM_PATH);
    var feaNum = 0;
    if(maxdimPathStr != null && maxdimPathStr != ""){
      val maxdimPath = new Path(maxdimPathStr)
      val fs = maxdimPath.getFileSystem(conf)

      if (!fs.exists(maxdimPath)) {
        LOG.error(s"$maxdimPath doesn't exist.")
      } else {
        val fin = fs.open(maxdimPath)
        val lr = new LineReader(fin)
        val line = new Text()
        lr.readLine(line)

        feaNum = Integer.valueOf(line.toString())
        lr.close()
      }
    } else{
      feaNum = conf.getInt(MLConf.ML_FEATURE_NUM, -1)
    }

    LOG.info("feanNum=" + feaNum)
    conf.setInt(MLConf.ML_FEATURE_NUM, feaNum)

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
    super.train(conf, LRModel(conf), classOf[LRTrainTask])
  }
}

