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
 */

package com.tencent.angel.spark.ops

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.linalg.SparseVector
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class SparseRowOpsSuite extends PSFunSuite with SharedPSContext {

  private val dim = 14
  private val capacity = 12
  private var _sparseRowOps: SparseRowOps = _
  var psVector: SparsePSVector = _
  var _sparseVector: SparsePSVector = _
  var _localVector: SparseVector = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // start Angel
    _sparseRowOps = PSClient.instance().sparseRowOps
    psVector = PSVector.sparse(dim, capacity)
  }

  override def afterAll(): Unit = {
    _sparseRowOps = null
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val rand = new Random()
    val indexSet = new mutable.HashSet[Long]()
    (0 until dim / 2).foreach { i =>
      indexSet.add(rand.nextInt(dim).toLong)
    }
    val indices = indexSet.toArray
    val values = indices.map(_ => rand.nextDouble())

    _localVector = new SparseVector(dim, indices, values)

    _sparseVector = PSVector.duplicate(psVector)

    _sparseVector.push(_localVector)
  }

  override def afterEach(): Unit = {
    super.afterEach()
  }

  test("pull some indices") {
    val indices = _localVector.indices.slice(0, dim / 2)

    val remoteValues = _sparseVector.pull(indices)

    indices.foreach { i =>
      assert(remoteValues(i) == _localVector(i))
    }
  }

  test("pull") {
    val remoteValues = _sparseVector.pull
    _localVector.indices.foreach { i =>
      assert(remoteValues(i) == _localVector(i))
    }
  }

  test("push") {
    val indices = Array[Long](0L, (dim / 2).toLong)
    val values = Array[Double](0.1, 0.5)
    val thisVector = new SparseVector(dim, indices, values)

    _sparseVector.push(thisVector)

    val remoteValues = _sparseVector.pull

    assert(remoteValues.nnz == indices.length)
    indices.foreach { i =>
      assert(thisVector(i) == remoteValues(i))
    }
  }

  test("increment") {
    val indices = Array[Long](0L, (dim / 2).toLong)
    val values = Array[Double](0.1, 0.5)
    val thisVector = new SparseVector(dim, indices, values)

    _sparseVector.increment(thisVector)

    val indicesSet = new mutable.HashSet[Long]()
    indices.foreach(i => indicesSet.add(i))
    _localVector.indices.foreach(i => indicesSet.add(i))

    val remoteValue = _sparseVector.pull
    indicesSet.foreach { i =>
      assert(remoteValue(i) == thisVector(i) + _localVector(i))
    }
  }
}
