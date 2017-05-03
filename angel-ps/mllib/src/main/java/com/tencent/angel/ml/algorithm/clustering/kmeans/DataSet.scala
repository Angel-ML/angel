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

package com.tencent.angel.ml.algorithm.clustering.kmeans

import java.io._
import java.util.{ArrayList, Random}

import com.tencent.angel.ml.math.vector.{SparseDoubleSortedVector, TDoubleVector}

class DataSet(var dim: Int) {
  //var samples = new ArrayList[TDoubleVector]()
  var samples : ArrayList[TDoubleVector] = _

  @throws[IOException]
  def read(path: String) {
    val reader = new BufferedReader(new FileReader(new File(path)))
    var line = new String
    samples = new ArrayList[TDoubleVector]()

    while ({line = reader.readLine; line != null}) {
      val sample = parseFromLibSVM(line, dim)
      samples.add(sample)
    }
  }

  def size: Int = samples.size

  def get(idx: Int): TDoubleVector = samples.get(idx)

  def randomOne: TDoubleVector = samples.get(new Random(System.currentTimeMillis).nextInt(samples.size))

  def parseFromLibSVM(line: String, dim: Int): TDoubleVector = {
    val parts = line.split(" ")
    val keys = new Array[Int](parts.length - 1)
    val vals = new Array[Double](parts.length - 1)

    for (i <- 1 until parts.length) {
      val kv = parts(i).split(":")
      val k = kv(0).toInt
      val v = kv(1).toDouble
      keys(i - 1) = k
      vals(i - 1) = v
    }

    new SparseDoubleSortedVector(dim, keys, vals)
  }
}
