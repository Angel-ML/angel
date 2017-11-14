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
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.factorizationmachines

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.SparseDoubleSortedVector
import com.tencent.angel.ml.task.TrainTask
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * This task train a Factorization Machines model
 *
  * @param ctx ： task context
  */
class FMTrainTask (val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  val LOG = LogFactory.getLog(classOf[FMTrainTask])
  val feaNum = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  var minP = Double.MaxValue
  var maxP = Double.MinValue
  val feaUsed = new Array[Int](feaNum)

  override def train(taskContext: TaskContext): Unit = {
    LOG.info("FM train task.")
    taskDataBlock.shuffle()

    val learner = new FMLearner(ctx, minP, maxP, feaUsed)
    learner.train(taskDataBlock, null)
  }

  /**
    * Parse each sample into a labeled data, of which X is the feature weight vector, Y is label.
    */
  override
  def parse(key: LongWritable, line: Text): LabeledData = {
    if (null == line) {
      return null
    }
    val splits = line.toString().trim().split(" ")
    if (splits.length < 1) {
      return null
    }
    val nonzero = splits.length - 1
    val x = new SparseDoubleSortedVector(nonzero, feaNum)
    val y = splits(0).toDouble

    //TODO edit y according to task type
    //classification: 1 && -1.
    // regression: real value.
    for (i : Int <- 1 until splits.length ) {
      val tmp = splits(i)
      val sep = tmp.indexOf(":")
      if (sep != -1) {
        val idx = tmp.substring(0, sep).toInt
        val value = tmp.substring(sep + 1).toDouble
        x.set(idx-1, value)
      }
    }
    new LabeledData(x, y)
  }

  override
  def preProcess(taskContext: TaskContext) {
    val reader = taskContext.getReader
    while (reader.nextKeyValue) {
      val data = parse(reader.getCurrentKey, reader.getCurrentValue)
      if (data != null) {
        taskDataBlock.put(data)
        val indexs = data.getX.asInstanceOf[SparseDoubleSortedVector].getIndices
        for (i <- indexs)
          feaUsed(i) += 1
        minP = if (data.getY < minP) data.getY else minP
        maxP = if (data.getY > maxP) data.getY else maxP
      }
    }
    taskDataBlock.flush()
    LOG.info(s"Preprocessed ${taskDataBlock.size()} samples. minP=$minP, maxP=$maxP, feaUsed" +
      s".size=${feaUsed.count({x:Int => x > 2})}")
  }

}
