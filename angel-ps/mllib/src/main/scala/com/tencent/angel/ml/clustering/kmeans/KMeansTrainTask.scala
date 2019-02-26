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


package com.tencent.angel.ml.clustering.kmeans

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.TrainTask
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.utils.LabeledData
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector, IntKeyVector, LongDoubleVector, LongFloatVector, LongKeyVector, Vector}
import com.tencent.angel.worker.storage.{DiskDataBlock, MemoryAndDiskDataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

import scala.util.Sorting.quickSort

class KMeansTrainTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  private val LOG = LogFactory.getLog(classOf[KMeansTrainTask])
  var idxsVector: Vector = _
  val indexRange: Long = SharedConf.indexRange
  private val valiRat = SharedConf.validateRatio

  // validation data storage
  val validDataBlock: DataBlock[LabeledData] = getDataBlock("memory")

  override def train(ctx: TaskContext): Unit = {
    LOG.info("#TrainSample=" + taskDataBlock.size)

    val learner = new KMeansLearner(ctx)
    learner.train(taskDataBlock, validDataBlock)
  }

  /**
    * Parse each sample into a labeled data, of which X is the feature weight vector, Y is label.
    */
  override def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }

  override def preProcess(taskContext: TaskContext) {
    val start = System.currentTimeMillis()

    var count = 0
    val vali = Math.ceil(1.0 / valiRat).toInt

    val reader = taskContext.getReader

    idxsVector = if (needIndexs) {
      val expected = Math.max(256, (SharedConf.indexRange / 10000).toInt)

      SharedConf.keyType() match {
        case "int" =>
          val idxs = new IntOpenHashSet(expected)
          while (reader.nextKeyValue) {
            val out = parse(reader.getCurrentKey, reader.getCurrentValue)
            if (out != null) {
              addIndexs(out.getX, idxs)

              if (count % vali == 0) {
                validDataBlock.put(out)
              } else {
                taskDataBlock.put(out)
              }
              count += 1
            }
          }
          set2Vector(idxs)
        case "long" =>
          val idxs = new LongOpenHashSet(expected)
          while (reader.nextKeyValue) {
            val out = parse(reader.getCurrentKey, reader.getCurrentValue)
            if (out != null) {
              addIndexs(out.getX, idxs)

              if (count % vali == 0) {
                validDataBlock.put(out)
              } else {
                taskDataBlock.put(out)
              }
              count += 1
            }
          }
          set2Vector(idxs)
      }
    } else {
      while (reader.nextKeyValue) {
        val out = parse(reader.getCurrentKey, reader.getCurrentValue)
        if (out != null) {
          if (count % vali == 0) {
            validDataBlock.put(out)
          } else {
            taskDataBlock.put(out)
          }
          count += 1
        }
      }

      null.asInstanceOf[Vector]
    }

    taskDataBlock.flush()
    validDataBlock.flush()

    val cost = System.currentTimeMillis() - start
    LOG.info(s"Task[${ctx.getTaskIndex}] preprocessed ${
      taskDataBlock.size + validDataBlock.size
    } samples, ${taskDataBlock.size} for train, " +
      s"${validDataBlock.size} for validation." +
      s" processing time is $cost"
    )
  }

  def getDataBlock(level: String = null): DataBlock[LabeledData] = {
    val storageLevel = if (level != null && level.length != 0) {
      level
    } else {
      SharedConf.storageLevel
    }

    if (storageLevel.equalsIgnoreCase("memory")) {
      new MemoryDataBlock[LabeledData](-1)
    } else if (storageLevel.equalsIgnoreCase("memory_disk")) {
      new MemoryAndDiskDataBlock[LabeledData](ctx.getTaskId.getIndex)
    } else {
      new DiskDataBlock[LabeledData](ctx.getTaskId.getIndex)
    }
  }

  protected def needIndexs: Boolean = {
    val inputFormat = SharedConf.inputDataFormat
    val modelType = SharedConf.storageType
    (inputFormat, modelType) match {
      case ("libsvm" | "dummy", "sparse" | "component_sparse") => false // true
      case ("dense", "libsvm" | "component_sparse") =>
        throw new AngelException("The input data is dense, but the model is sparse!")
      case _ => false
    }
  }

  protected def addIndexs(vector: Vector, idxs: IntOpenHashSet): Unit = {
    vector match {
      case v: IntDoubleVector if !v.isDense =>
        v.getStorage.getIndices.foreach { i => idxs.add(i) }
      case v: IntFloatVector if !v.isDense =>
        v.getStorage.getIndices.foreach { i => idxs.add(i) }
      case v: IntKeyVector if v.isDense =>
        (0 until v.getDim).foreach { i => idxs.add(i) }
    }
  }

  protected def addIndexs(vector: Vector, idxs: LongOpenHashSet): Unit = {
    vector match {
      case v: LongDoubleVector if !v.isDense =>
        v.getStorage.getIndices.foreach { i => idxs.add(i) }
      case v: LongFloatVector if !v.isDense =>
        v.getStorage.getIndices.foreach { i => idxs.add(i) }
      case v: LongKeyVector if v.isDense =>
        (0L until v.getDim).foreach { i => idxs.add(i) }
    }
  }

  protected def set2Vector(idxs: LongOpenHashSet): Vector = {
    val temp = idxs.toLongArray
    quickSort(temp)
    VFactory.denseLongVector(temp)
  }

  protected def set2Vector(idxs: IntOpenHashSet): Vector = {
    val temp = idxs.toIntArray
    quickSort(temp)
    VFactory.denseIntVector(temp)
  }
}
