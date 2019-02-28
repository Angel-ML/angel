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

package com.tencent.angel.spark.automl.feature.cross

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}

import scala.collection.mutable.ArrayBuffer

object FeatureCrossOp {

  def flatCartesian(vector: Vector): Vector = {
    val curDim = vector.size
    vector match {
      case sv: SparseVector =>
        val indices = new ArrayBuffer[Int]()
        val values = new ArrayBuffer[Double]()
        sv.indices.foreach { idx1 =>
          sv.indices.foreach { idx2 =>
            indices += curDim * idx1 + idx2
            values += sv(idx1) * sv(idx2)
          }
        }
        val sorted = indices.zip(values).sortBy(_._1)
        val sortedIndices = sorted.map(_._1)
        val sortedValues = sorted.map(_._2)
        new SparseVector(sv.size * sv.size, sortedIndices.toArray, sortedValues.toArray)
      case dv: DenseVector =>
        val values: Array[Double] = new Array(dv.size * dv.size)
        (0 until dv.size).foreach { idx1 =>
          (0 until dv.size).foreach { idx2 =>
            values(dv.size * idx1 + idx2) = dv(idx1) * dv(idx2)
          }
        }
        new DenseVector(values)
    }
  }

  def main(args: Array[String]): Unit = {
    val v = new DenseVector(Array(1, 2, 3))
    val cv = flatCartesian(v)
    println(cv.toDense.values.mkString(","))
  }

}
