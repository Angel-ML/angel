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


package com.tencent.angel.ml.GBDT

import com.tencent.angel.ml.core.TrainTask
import com.tencent.angel.ml.core.conf.AngelMLConf
import com.tencent.angel.ml.math2.utils.LabeledData
import com.tencent.angel.worker.storage.MemoryDataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

class GBDTTrainTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  private val LOG = LogFactory.getLog(classOf[GBDTTrainTask])

  private val indexRange: Long = conf.getLong(AngelMLConf.ML_FEATURE_INDEX_RANGE, AngelMLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  private val validRatio = conf.getDouble(AngelMLConf.ML_VALIDATE_RATIO, AngelMLConf.DEFAULT_ML_VALIDATE_RATIO)

  // validation data storage
  var validDataStorage = new MemoryDataBlock[LabeledData](-1)

  /**
    * @param ctx : task context
    */
  @throws[Exception]
  def train(ctx: TaskContext) {
    val trainer = new GBDTLearner(ctx)
    trainer.trainOld(taskDataBlock, validDataStorage)
  }

  /**
    * parse the input text to trainning data
    *
    * @param key   the key
    * @param value the text
    */
  def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
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
          taskDataBlock.put(out)
        count += 1
      }
    }
    taskDataBlock.flush()
    validDataStorage.flush()
  }

}