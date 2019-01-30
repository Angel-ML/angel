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


package com.tencent.angel.ml.core

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{AngelMLConf, SharedConf}
import com.tencent.angel.ml.core.data.{DataParser, TransLabel}
import com.tencent.angel.ml.core.utils.SConfHelper
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.utils.LabeledData
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector, IntKeyVector, LongDoubleVector, LongFloatVector, LongKeyVector, Vector}
import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import it.unimi.dsi.fastutil.longs.LongOpenHashSet

import scala.util.Sorting.quickSort

/**
  * The type labeled base task.
  */
abstract class TrainTask[KEYIN, VALUEIN](taskContext: TaskContext)
  extends BaseTask[KEYIN, VALUEIN, LabeledData](taskContext) with SConfHelper {
  val sharedConf: SharedConf = initConf(conf)
  private val transLabel: TransLabel = TransLabel.get(
    sharedConf.getString(AngelMLConf.ML_DATA_LABEL_TRANS, AngelMLConf.DEFAULT_ML_DATA_LABEL_TRANS),
    sharedConf.getDouble(AngelMLConf.ML_DATA_LABEL_TRANS_THRESHOLD, AngelMLConf.DEFAULT_ML_DATA_LABEL_TRANS_THRESHOLD)
  )
  protected val dataParser = DataParser(SharedConf.indexRange, SharedConf.inputDataFormat,
    sharedConf.getString(AngelMLConf.ML_DATA_SPLITOR, AngelMLConf.DEFAULT_ML_DATA_SPLITOR),
    SharedConf.modelType, sharedConf.getBoolean(AngelMLConf.ML_DATA_HAS_LABEL, AngelMLConf.DEFAULT_ML_DATA_HAS_LABEL),
    true, transLabel)

  final def run(taskContext: TaskContext): Unit = {
    this.train(taskContext)
  }

  def train(taskContext: TaskContext)

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
