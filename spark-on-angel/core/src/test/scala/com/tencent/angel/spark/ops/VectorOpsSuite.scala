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

import scala.{math => SMath}

import com.tencent.angel.spark._
import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.linalg.DenseVector
import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector
import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector.blas
import com.tencent.angel.spark.models.vector.{DensePSVector, PSVector}
import com.tencent.angel.spark.pof._

class VectorOpsSuite extends PSFunSuite with SharedPSContext {
  private val dim = 14
  private val capacity = 12
  private var _vectorOps: VectorOps = _
  var psVector: DensePSVector = _
  var zeroVector: DensePSVector = _
  var uniformVector: DensePSVector = _
  var normalVector: DensePSVector = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // start Angel
    _vectorOps = PSClient.instance().vectorOps

    // create pool
    psVector = PSVector.dense(dim, capacity)
  }

  override def afterAll(): Unit = {
    _vectorOps = null
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    zeroVector = PSVector.duplicate(psVector)
    uniformVector = PSVector.duplicate(psVector).randomUniform(0.0, 1.0)
    normalVector = PSVector.duplicate(psVector).randomNormal(0.0, 1.0)
  }

  override def afterEach(): Unit = {
    normalVector.delete()
    uniformVector.delete()
    zeroVector.delete()
    super.afterEach()
  }

  test("doSum") {
    assert(uniformVector.pull.values.sum === _vectorOps.sum(uniformVector))
  }

  test("doMax") {
    assert(normalVector.pull.values.max === _vectorOps.max(normalVector))
  }

  test("doMin") {
    assert(normalVector.pull.values.min === _vectorOps.min(normalVector))
  }

  test("doNnz") {
    val array = Array.fill[Double](dim)(0.0)
    array.update(3, 1.0)
    array.update(0, 1.0)
    psVector.push(new DenseVector(array))

    assert(_vectorOps.nnz(psVector) == 2)
  }

  test("doAdd") {
    val constNum = 3.14
    psVector = BreezePSVector.canAddS(uniformVector.toBreeze, constNum).toDense

    val result = uniformVector.pull.values.map(_ + constNum)
    assert(psVector.pull.values.sameElements(result))
  }

  test("doMul") {
    val constNum = 3.14
    psVector = BreezePSVector.canMulS(uniformVector.toBreeze, constNum).toDense

    val result = uniformVector.pull.values.map(_ * constNum)
    assert(psVector.pull.values.sameElements(result))
  }

  test("doDiv") {
    val constNum = 3.14
    psVector = BreezePSVector.canDivS(uniformVector.toBreeze, constNum).toDense

    val result = uniformVector.pull.values.map(_ / constNum)
    assert(psVector.pull.values.sameElements(result))
  }

  test("doPow") {
    val constNum = 3.14
    psVector = BreezePSVector.math.pow(uniformVector.toBreeze, constNum).toDense

    val result = uniformVector.pull.values.map(SMath.pow(_, constNum))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doSqrt") {
    psVector = BreezePSVector.math.sqrt(uniformVector.toBreeze).toDense

    val result = uniformVector.pull.values.map(x => SMath.sqrt(x))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doExp") {
    psVector = BreezePSVector.math.exp(uniformVector.toBreeze).toDense

    val result = uniformVector.pull.values.map(x => SMath.exp(x))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doExpm1") {
    psVector = BreezePSVector.math.expm1(uniformVector.toBreeze).toDense

    val result = uniformVector.pull.values.map(x => SMath.expm1(x))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doLog") {
    psVector = BreezePSVector.math.log(uniformVector.toBreeze).toDense

    val result = uniformVector.pull.values.map(x => SMath.log(x))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doLog1p") {
    psVector = BreezePSVector.math.log1p(uniformVector.toBreeze).toDense

    val result = uniformVector.pull.values.map(x => SMath.log1p(x))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doLog10") {
    psVector = BreezePSVector.math.log10(uniformVector.toBreeze).toDense

    val result = uniformVector.pull.values.map(x => SMath.log10(x))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doCeil") {
    psVector = BreezePSVector.math.ceil(uniformVector.toBreeze).toDense

    val result = uniformVector.pull.values.map(x => SMath.ceil(x))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doFloor") {
    psVector = BreezePSVector.math.floor(uniformVector.toBreeze).toDense

    val result = uniformVector.pull.values.map(x => SMath.floor(x))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doRound") {
    psVector = BreezePSVector.math.round(uniformVector.toBreeze).toDense

    val result = uniformVector.pull.values.map(x => SMath.round(x))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doAbs") {
    psVector = BreezePSVector.math.abs(uniformVector.toBreeze).toDense

    val result = uniformVector.pull.values.map(x => SMath.abs(x))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doSignum") {
    psVector = BreezePSVector.math.signum(uniformVector.toBreeze).toDense

    val result = uniformVector.pull.values.map(x => SMath.signum(x))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doAdd two PSVectors") {
    _vectorOps.add(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.pull.values
    val normalArray = normalVector.pull.values
    val result = (0 until dim).toArray.map(i => uniArray(i) + normalArray(i))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doSub two PSVectors") {
    _vectorOps.sub(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.pull.values
    val normalArray = normalVector.pull.values
    val result = (0 until dim).toArray.map(i => uniArray(i) - normalArray(i))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doMul two PSVectors") {
    _vectorOps.mul(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.pull.values
    val normalArray = normalVector.pull.values
    val result = (0 until dim).toArray.map(i => uniArray(i) * normalArray(i))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doDiv two PSVectors") {
    _vectorOps.div(normalVector, uniformVector, psVector)

    val normalArray = normalVector.pull.values
    val uniArray = uniformVector.pull.values
    val result = (0 until dim).toArray.map(i => normalArray(i) / uniArray(i))
    assert(psVector.pull.values.sameElements(result))
  }

  test("doMax two PSVectors") {
    _vectorOps.max(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.pull.values
    val normalArray = normalVector.pull.values
    val result = (0 until dim).toArray.map (i =>
      if (uniArray(i) > normalArray(i)) uniArray(i) else normalArray(i)
    )
    assert(psVector.pull.values.sameElements(result))
  }

  test("doMin two PSVectors") {
    _vectorOps.min(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.pull.values
    val normalArray = normalVector.pull.values
    val result = (0 until dim).toArray.map (i =>
      if (uniArray(i) < normalArray(i)) uniArray(i) else normalArray(i)
    )
    assert(psVector.pull.values.sameElements(result))
  }

  test("doMap") {
    val multiplier = 2.0
    _vectorOps.map(normalVector, new MapFuncTest(multiplier), psVector)

    val result = normalVector.pull.values.map(x => multiplier * x * x)
    assert(psVector.pull.values.sameElements(result))
  }

  test("doZip2Map") {
    val multiplier = 2.0
    _vectorOps.zip2Map(uniformVector, normalVector, new Zip2MapFuncTest(multiplier), psVector)

    val uniformArray = uniformVector.pull.values
    val normalArray = normalVector.pull.values
    val result = (0 until dim).toArray.map { i =>
      multiplier * uniformArray(i) + normalArray(i) * normalArray(i)
    }

    assert(psVector.pull.values.sameElements(result))
  }

  test("doZip3Map") {
    val multiplier = 2.0
    _vectorOps.zip3Map(uniformVector, normalVector, normalVector, new Zip3MapFuncTest(multiplier), psVector)

    val uniformArray = uniformVector.pull.values
    val normalArray = normalVector.pull.values
    val result = (0 until dim).toArray.map { i =>
      multiplier * uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)
    }

    assert(psVector.pull.values.sameElements(result))
  }

  test("doMapWithIndex") {
    val multiplier = 2.0
    _vectorOps.mapWithIndex(normalVector, new MapWithIndexFuncTest(multiplier), psVector)

    val normalArray = normalVector.pull.values
    val result = (0 until dim).toArray.map { i =>
      if (i == 0) {
        normalArray(i) * normalArray(i)
      } else {
        multiplier * normalArray(i) * normalArray(i)
      }
    }

    assert(psVector.pull.values.sameElements(result))
  }

  test("doZip2MapWithIndex") {
    val multiplier = 2.0
    _vectorOps.zip2MapWithIndex(uniformVector, normalVector, new Zip2MapWithIndexFuncTest(multiplier), psVector)

    val uniformArray = uniformVector.pull.values
    val normalArray = normalVector.pull.values
    val result = (0 until dim).toArray.map {i =>
      if (i == 0) {
        uniformArray(i) + normalArray(i) * normalArray(i)
      } else {
        multiplier * uniformArray(i) + normalArray(i) * normalArray(i)
      }
    }

    assert(psVector.pull.values.sameElements(result))
  }

  test("doZip3MapWithIndex") {
    val multiplier = 2.0
    _vectorOps.zip3MapWithIndex(uniformVector, normalVector, normalVector, new Zip3MapWithIndexFuncTest(multiplier), psVector)

    val uniformArray = uniformVector.pull.values
    val normalArray = normalVector.pull.values
    val result = (0 until dim).toArray.map { i =>
      if (i == 0) {
        uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)
      } else {
        multiplier * uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)

      }
    }

    assert(psVector.pull.values.sameElements(result))
  }

  test("doAxpy") {
    val uniformArray = uniformVector.pull.values
    val normalArray = normalVector.pull.values
    val result = (0 until dim).toArray.map { i =>
      normalArray(i) + 2.0 * uniformArray(i)
    }

    _vectorOps.axpy(2.0, uniformVector, normalVector)

    (0 until dim).foreach { i =>
      assert(normalVector.pull.values(i) === result(i))
    }
  }

  test("doDot") {
    val dot = _vectorOps.dot(uniformVector, normalVector)
    val uniformArray = uniformVector.pull.values
    val normalArray = normalVector.pull.values
    val result = (0 until dim).toArray.map { i =>
      normalArray(i) * uniformArray(i)
    }.sum
    assert(dot === result)
  }

  test("doCopy") {
    _vectorOps.copy(normalVector, psVector)

    assert(psVector.pull.values.sameElements(normalVector.pull.values))
  }

  test("doScal") {
    val result = normalVector.pull.values.map(_ * -0.1)
    blas.scal(-0.1, normalVector.toBreeze)
    val scale = normalVector.pull.values

    (0 until dim).foreach { i =>
      assert(scale(i) === result(i))
    }
  }

  test("doNrm2") {
    val norm = _vectorOps.nrm2(normalVector)
    val result = SMath.sqrt(normalVector.pull.values.map(x => x * x).sum)
    assert(result === norm)
  }

  test("doAsum") {
    val asum = _vectorOps.asum(normalVector)
    val result = normalVector.pull.values.map(SMath.abs).sum
    assert(result === asum)
  }

  test("doAmax") {
    val amax = _vectorOps.amax(normalVector)
    val result = normalVector.pull.values.map(SMath.abs).max
    assert(result === amax)
  }

  test("doAmin") {
    val result = normalVector.pull.values.map(SMath.abs).min
    val amin = _vectorOps.amin(normalVector)

    assert(result === amin)
  }

  test("doFill") {
    val psProxy = PSVector.duplicate(psVector).randomUniform(0.0, 1.0)

    _vectorOps.fill(psProxy, 3.14)
    assert(psProxy.pull.values.sameElements(Array.fill[Double](dim)(3.14)))
  }



}
