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


package com.tencent.angel.spark.models

import scala.util.Random

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntFloatVector, LongFloatVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class PSVectorSuite extends PSFunSuite with SharedPSContext {

  private val dim = 10
  private val capacity = 10
  private var _psContext: PSContext = _
  private var _psVector: PSVector = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _psContext = PSContext.instance()
    _psVector = PSVector.dense(dim, capacity, RowType.T_FLOAT_DENSE)
  }

  override def afterAll(): Unit = {
    _psContext.destroyVectorPool(_psVector)
    super.afterAll()
  }


  test("delete") {
    val pool = PSContext.instance().asInstanceOf[AngelPSContext]
      .getPool(_psVector.poolId)
    val poolSize = pool.size
    val ps = PSVector.duplicate(_psVector)
    assert(pool.size == poolSize + 1)
    ps.delete()
    assert(pool.size == poolSize)
  }

  test("duplicate Vector") {
    val dVector = PSVector.duplicate(_psVector)
    assert(dVector.poolId == _psVector.poolId)
    assert(dVector.id != _psVector.id)
  }

  test("new dense vector") {
    val dVector = PSVector.dense(dim, capacity)
    assert(dVector.poolId != _psVector.poolId)
  }


  test("new sparse vector") {
    val sVector = PSVector.sparse(dim, capacity)
    assert(sVector.isInstanceOf[PSVector])
  }

  test("pull & push & increment & update"){
    val randomVector = VFactory.denseFloatVector(Array.tabulate(dim)(_ => Random.nextFloat()))
    _psVector.push(randomVector)
    var ret = _psVector.pull().asInstanceOf[IntFloatVector]
    for(i <- 0 until dim){
      assert(ret.get(i) == randomVector.get(i))
    }

    var indices = Array.tabulate(5)(_ => Random.nextInt(dim)).distinct
    ret = _psVector.pull(indices).asInstanceOf[IntFloatVector]
    for(i <- 0 until dim){
      if(indices.contains(i))
        assert(ret.get(i) == randomVector.get(i))
      else
        assert(ret.get(i) == 0.0f)
    }

    _psVector.increment(randomVector)
    ret = _psVector.pull().asInstanceOf[IntFloatVector]
    for(i <- 0 until dim){
      assert(ret.get(i) == 2 * randomVector.get(i))
    }

    indices = Array.tabulate(5)(_ => Random.nextInt(dim)).distinct
    val values = indices.map(_ => Random.nextFloat())
    _psVector.update(VFactory.sparseFloatVector(dim, indices, values))
    ret = _psVector.pull().asInstanceOf[IntFloatVector]
    for(i <- indices.indices) {
      assert(ret.get(indices(i)) == values(i))
    }
    for(i <- 0 until dim){
      if(!indices.contains(i)){
        assert(ret.get(i) == 2 * randomVector.get(i))
      }
    }
  }

  test("pull with Long indices"){
    val mat = PSVector.sparse(dim, 2, RowType.T_FLOAT_SPARSE_LONGKEY)
    val indices = Array[Long](3, 2, 5, 7, 1)
    val indices_copy = new Array[Long](5)
    indices.copyToArray(indices_copy)
    val values = indices.map(_ => Random.nextFloat())
    mat.increment(VFactory.sparseLongKeyFloatVector(dim, indices_copy, values))

    val ret = mat.pull(indices_copy).asInstanceOf[LongFloatVector]
    for(i <- indices.indices) {
      assert(ret.get(indices(i)) == values(i))
    }
    for(i <- 0 until dim){
      if(!indices.contains(i)){
        assert(ret.get(i) == 0.0f)
      }
    }
  }

  test("fill"){
    val r = Random.nextDouble()
    _psVector.fill(r)
    val ret = _psVector.pull().asInstanceOf[IntFloatVector]
    for(i <- 0 until dim){
      assert(math.abs(ret.get(i) - r) < 1e-6)
    }
  }

  test("type compatible"){
    // longKey
    val longKeyMat = PSVector.sparse(dim, 2, RowType.T_FLOAT_SPARSE_LONGKEY)
    val intKeyMat = PSVector.sparse(dim, 2, RowType.T_FLOAT_SPARSE)
    val longKeyIndices = Array.tabulate(dim)(_ => Random.nextInt(dim).toLong).distinct
    val intKeyIndices = Array.tabulate(dim)(_ => Random.nextInt(dim)).distinct
    val longKeyDoubleVector = VFactory.sparseLongKeyDoubleVector(dim, longKeyIndices, longKeyIndices.map(_ => Random.nextDouble()))
    val longKeyFloatVector = VFactory.sparseLongKeyFloatVector(dim, longKeyIndices, longKeyIndices.map(_ => Random.nextFloat()))
    val intKeyDoubleVector = VFactory.sparseDoubleVector(dim, intKeyIndices, intKeyIndices.map(_ => Random.nextDouble()))

    // pull longKey ServerRow by intKey indices
    intercept[IllegalArgumentException] {
      longKeyMat.pull(Array(1, 2, 3))
    }

    // pull intKey ServerRow by longKey indices
    intercept[IllegalArgumentException] {
      intKeyMat.pull(Array[Long](1, 2, 3))
    }

    // push Double vector to Float ServerRow
    intercept[IllegalArgumentException] {
      longKeyMat.push(longKeyDoubleVector)
    }

    // increment Double vector to Float ServerRow
    intercept[IllegalArgumentException] {
      longKeyMat.increment(longKeyDoubleVector)
    }

    // update Double vector to Float ServerRow
    intercept[IllegalArgumentException] {
      longKeyMat.update(longKeyDoubleVector)
    }

    // push intKey vector to longKey ServerRow
    intercept[IllegalArgumentException] {
      longKeyMat.push(intKeyDoubleVector)
    }

    // increment intKey vector to longKey ServerRow
    intercept[IllegalArgumentException] {
      longKeyMat.increment(intKeyDoubleVector)
    }

    // update intKey vector to longKey ServerRow
    intercept[IllegalArgumentException] {
      longKeyMat.update(intKeyDoubleVector)
    }

    // push longKey vector to intKey ServerRow
    intercept[IllegalArgumentException] {
      intKeyMat.push(longKeyFloatVector)
    }

    // increment longKey vector to intKey ServerRow
    intercept[IllegalArgumentException] {
      intKeyMat.increment(longKeyFloatVector)
    }

    // update longKey vector to intKey ServerRow
    intercept[IllegalArgumentException] {
      intKeyMat.update(longKeyFloatVector)
    }


  }

}