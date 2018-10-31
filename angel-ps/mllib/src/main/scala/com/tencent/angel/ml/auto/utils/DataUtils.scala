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


package com.tencent.angel.ml.auto.utils

import com.tencent.angel.ml.math2.vector.IntFloatVector
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object DataUtils {

  def parse(sc: SparkContext, X: List[IntFloatVector], y: List[Float]): RDD[LabeledPoint] = {
    sc.parallelize(X.zipWithIndex.map(entry => parse(entry._1, y(entry._2))))
  }

  def parse(feature: IntFloatVector, label: Float): LabeledPoint = {
    if (feature.isDense)
      LabeledPoint(label, Vectors.dense(feature.getStorage.getValues.map(x => x.toDouble)))
    else
      LabeledPoint(label, Vectors.sparse(feature.getDim, feature.getStorage.getIndices, feature.getStorage.getValues.map(x => x.toDouble)))
  }

  def parse(feature: IntFloatVector): Vector = {
    if (feature.isDense)
      Vectors.dense(feature.getStorage.getValues.map(x => x.toDouble))
    else
      Vectors.sparse(feature.numZeros, feature.getStorage.getIndices, feature.getStorage.getValues.map{x => x.toDouble})
  }
}
