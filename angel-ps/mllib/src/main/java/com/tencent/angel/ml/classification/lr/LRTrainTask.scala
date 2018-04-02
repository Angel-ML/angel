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
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.task.TrainTask
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.worker.storage.MemoryDataBlock
import com.tencent.angel.worker.task.TaskContext
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.io.{LongWritable, Text}

import scala.math.Numeric
import scala.reflect.runtime.universe._


/**
  * Binomial logistic regression is a linear classification algorithm, each sample is labeled as
  * positive(+1) or negtive(-1). In this algorithm, the probability describing a single sample
  * being drawn from the positive class is modeled using a logistic function:
  * P(Y=+1|X) = 1 / [1+ exp(-dot(w,x))]. This task learns a binomial logistic regression model
  * with mini-batch gradient descent.
  *
  * @param ctx : task context
  **/
class LRTrainTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  val LOG: Log = LogFactory.getLog(classOf[LRTrainTask])

  // feature number of training data
  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  private val valiRat = conf.getDouble(MLConf.ML_VALIDATE_RATIO, MLConf.DEFAULT_ML_VALIDATE_RATIO)

  // validation data storage
  var validDataBlock = new MemoryDataBlock[LabeledData](-1)

  // Enable index get
  private val enableIndexGet = conf.getBoolean(MLConf.ML_PULL_WITH_INDEX_ENABLE, MLConf.DEFAULT_ML_PULL_WITH_INDEX_ENABLE)
  // data format of training data, libsvm or dummy
  override val dataParser = DataParser(conf)
  val modelType: RowType = RowType.valueOf(conf.get(MLConf.ML_MODEL_TYPE, MLConf.DEFAULT_ML_MODEL_TYPE))
  var indexes: Any = _

  override def train(ctx: TaskContext) {
    modelType match {
      case RowType.T_DOUBLE_SPARSE_LONGKEY | RowType.T_FLOAT_SPARSE_LONGKEY | RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT =>
        train(ctx: TaskContext, indexes.asInstanceOf[Array[Long]])
      case _ => train(ctx: TaskContext, indexes.asInstanceOf[Array[Int]])
    }
  }

  def train[N: Numeric : TypeTag](ctx: TaskContext, indexes: Array[N]): Unit = {
    val trainer = new LRLearner(ctx)
    LOG.info("Begin to train ...")
    trainer.train(taskDataBlock, validDataBlock, indexes)
  }

  override def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }

  override def preProcess(taskContext: TaskContext) {
    val start = System.currentTimeMillis()

    var count = 0
    val vali = Math.ceil(1.0 / valiRat).toInt

    val reader = taskContext.getReader
    val indexSet = modelType match {
      case RowType.T_DOUBLE_SPARSE_LONGKEY | RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT | RowType.T_FLOAT_SPARSE_LONGKEY =>
        new LongOpenHashSet()
      case _ => new IntOpenHashSet()
    }

    while (reader.nextKeyValue) {
      val out = parse(reader.getCurrentKey, reader.getCurrentValue)
      if (out != null) {
        if (enableIndexGet) {
          updateIndex(out, indexSet)
        }
        if (count % vali == 0)
          validDataBlock.put(out)
        else
          taskDataBlock.put(out)
        count += 1
      }
    }
    if (enableIndexGet && !indexSet.isEmpty) {
      indexes = modelType match {
        case RowType.T_DOUBLE_SPARSE_LONGKEY | RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT | RowType.T_FLOAT_SPARSE_LONGKEY =>
          val array = indexSet.asInstanceOf[LongOpenHashSet].toLongArray
          LOG.info("after preprocess data , index length = " + array.length)
          array
        case _ =>
          val array = indexSet.asInstanceOf[IntOpenHashSet].toIntArray
          LOG.info("after preprocess data , index length = " + array.length)
          array
      }
    }

    taskDataBlock.flush()
    validDataBlock.flush()

    val cost = System.currentTimeMillis() - start
    LOG.info(s"Task[${ctx.getTaskIndex}] preprocessed ${
      taskDataBlock.size +
        validDataBlock.size
    } samples, ${taskDataBlock.size} for train, " +
      s"${validDataBlock.size} for validation. " +
      s" processing time is $cost"
    )
  }

  def updateIndex(labeledData: LabeledData, indexSet: Any): Unit = {
    if (enableIndexGet) {
      (labeledData.getX, indexSet) match {
        case (x: SparseDummyVector, index: IntOpenHashSet) => x.getIndices.foreach { idx => index.add(idx) }
        case (x: SparseDoubleVector, index: IntOpenHashSet) => x.getIndices.foreach { idx => index.add(idx) }
        case (x: SparseFloatVector, index: IntOpenHashSet) => x.getIndices.foreach { idx => index.add(idx) }
        case (x: SparseDoubleSortedVector, index: IntOpenHashSet) => x.getIndices.foreach { idx => index.add(idx) }
        case (x: SparseFloatSortedVector, index: IntOpenHashSet) => x.getIndices.foreach { idx => index.add(idx) }
        case (x: SparseFloatSortedVector, index: IntOpenHashSet) => x.getIndices.foreach { idx => index.add(idx) }
        case (x: SparseLongKeyDummyVector, index: LongOpenHashSet) => x.getIndexes.foreach { idx => index.add(idx) }
        case (x: SparseLongKeySortedDoubleVector, index: LongOpenHashSet) => x.getIndexes.foreach { idx => index.add(idx) }
        case (x: SparseLongKeySortedFloatVector, index: LongOpenHashSet) => x.getIndexes.foreach { idx => index.add(idx) }
      }
    }
  }

}
