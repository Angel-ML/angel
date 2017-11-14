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


import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.task.TrainTask
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.worker.storage.MemoryDataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.io.{LongWritable, Text}


/**
  * Binomial logistic regression is a linear classification algorithm, each sample is labeled as
  * positive(+1) or negtive(-1). In this algorithm, the probability describing a single sample
  * being drawn from the positive class is modeled using a logistic function:
  * P(Y=+1|X) = 1 / [1+ exp(-dot(w,x))]. This task learns a binomial logistic regression model
  * with mini-batch gradient descent.
  *
  * @param ctx: task context
  **/
class LRTrainTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  val LOG: Log = LogFactory.getLog(classOf[LRTrainTask])

  // feature number of training data
  private val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  // data format of training data, libsvm or dummy
  private val dataFormat = conf.get(MLConf.ML_DATA_FORMAT, "dummy")
  // validate sample ratio
  private val valiRat = conf.getDouble(MLConf.ML_VALIDATE_RATIO, 0.05)


  private val dataParser = DataParser(dataFormat, feaNum, true)

  // validation data storage
  var validDataBlock = new MemoryDataBlock[LabeledData](-1)


  override
  def train(ctx: TaskContext) {
    val trainer = new LRLearner(ctx)
    trainer.train(taskDataBlock, validDataBlock)
  }

  /**
    * parse the input text to trainning data
    *
    * @param key   the key
    * @param value the text
    */
  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString())
  }

  /**
    * before trainning, preprocess input text to trainning data and put them into trainning data
    * storage and validation data storage separately
    */
  override
  def preProcess(taskContext: TaskContext) {
    val start = System.currentTimeMillis()

    var count = 0
    val vali = Math.ceil(1.0 / valiRat).asInstanceOf[Int]

    val reader = taskContext.getReader

    while (reader.nextKeyValue) {
      val out = parse(reader.getCurrentKey, reader.getCurrentValue)
      if (out != null) {
        if (count % vali == 0)
          validDataBlock.put(out)
        else
          taskDataBlock.put(out)
        count += 1
      }
    }
    taskDataBlock.flush()
    validDataBlock.flush()

    val cost = System.currentTimeMillis() - start
    LOG.info(s"Task[${ctx.getTaskIndex}] preprocessed ${taskDataBlock.size +
      validDataBlock.size} samples, ${taskDataBlock.size} for train, " +
      s"${validDataBlock.size} for validation. feanum=$feaNum")
  }

}