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

package com.tencent.angel.ml.algorithm.classification.svm

import com.tencent.angel.ml.algorithm.classification.lr.LRLeaner
import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.algorithm.utils.DataParser
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.worker.storage.MemoryStorage
import com.tencent.angel.worker.task.{TaskContext, TrainTask}
import org.apache.hadoop.io.{LongWritable, Text}

class SVMTrainTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {

  // feature number of trainning data
  private val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  // data format of trainning data, libsvm or dummy
  private val dataFormat = conf.get(MLConf.ML_DATAFORMAT, "dummy")
  // validate sample ratio
  private val validRatio = conf.getDouble(MLConf.ML_VALIDATE_RATIO, 0.05)

  // validation data storage
  var validDataStorage = new MemoryStorage[LabeledData](-1)

  /**
    * @param ctx: task context
    */
  @throws[Exception]
  def run(ctx: TaskContext) {
    val trainer = new LRLeaner(ctx)
    trainer.train(trainDataStorage, validDataStorage)
  }

  /**
    * parse the input text to trainning data
    *
    * @param key   the key
    * @param value the text
    */
  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    val sample = DataParser.parse(key, value, feaNum, "train", dataFormat)
    sample
  }

  /**
    * before trainning, preprocess input text to trainning data and put them into trainning data
    * storage and validation data storage separately
    */
  override
  def preProcess(taskContext: TaskContext) {
    var count = 0
    val valid = Math.ceil(1.0 / validRatio).asInstanceOf[Int]

    val reader = taskContext.getReader

    while (reader.nextKeyValue) {
      val out = parse(reader.getCurrentKey, reader.getCurrentValue)
      if (out != null) {
        if (count % valid == 0)
          validDataStorage.put(out)
        else
          trainDataStorage.put(out)
        count += 1
      }
    }

    trainDataStorage.flush()
    validDataStorage.flush()
  }

}