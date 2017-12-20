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

import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.{math => SMath}

import com.tencent.angel.spark._
import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.models.vector.{DensePSVector, PSVector, SparsePSVector}
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

  test("doPull") {
    assert(_vectorOps.pull(normalVector).sameElements(normalVector.pull()))
  }

  test("doIncrement dense") {
    val localArray = Array.fill[Double](dim)(0.1)

    val oldPSArray = normalVector.pull()
    val result = localArray.indices.map(i => oldPSArray(i) + localArray(i))

    _vectorOps.increment(normalVector, localArray)

    assert(normalVector.pull().sameElements(result))
  }

  test("doIncrement sparse") {
    val localPair = new ArrayBuffer[(Long, Double)]()
    val rand = new Random()
    val indics = new HashSet[Long]()
    (0 until dim / 2).foreach { i =>
      indics.add(rand.nextInt(dim).toLong)
    }
    indics.foreach { i => localPair.append(Tuple2(i, rand.nextDouble())) }

    val result = new Array[Double](dim)
    localPair.foreach { case (index, value) =>
      result(index.toInt) += value
    }

    val sVector = SparsePSVector(dim)

    PSClient.instance().sparseRowOps.increment(sVector, localPair.toArray)
    val pullVector = new Array[Double](dim)
    sVector.sparsePull().foreach { case (index, value) => pullVector(index.toInt) = value }
    assert(pullVector.sameElements(result))
  }

  test("doMergeMax") {
    val localPSVector = normalVector.pull()

    val localArray = Array.fill[Double](dim)(0.1)
    _vectorOps.mergeMax(normalVector, localArray)

    val max = localArray.indices.map { i =>
      if (localArray(i) > localPSVector(i)) localArray(i) else localPSVector(i)
    }

    assert(normalVector.pull().sameElements(max))
  }

  test("doMergeMin") {
    val localPSVector = normalVector.pull()

    val localArray = Array.fill[Double](dim)(0.1)
    _vectorOps.mergeMin(normalVector, localArray)

    val min = localArray.indices.map { i =>
      if (localArray(i) < localPSVector(i)) localArray(i) else localPSVector(i)
    }

    assert(normalVector.pull().sameElements(min))
  }

  test("doPush") {
    val psProxy = PSVector.duplicate(psVector)
    val localArray = Array.fill[Double](dim)(3.14)
    _vectorOps.push(psProxy, localArray)
    assert(psProxy.pull().sameElements(localArray))
  }

  test("doSum") {
    assert(uniformVector.pull().sum === _vectorOps.sum(uniformVector))
  }

  test("doMax") {
    assert(normalVector.pull().max === _vectorOps.max(normalVector))
  }

  test("doMin") {
    assert(normalVector.pull().min === _vectorOps.min(normalVector))
  }

  test("doNnz") {
    val array = Array.fill[Double](dim)(0.0)
    array.update(3, 1.0)
    array.update(0, 1.0)
    _vectorOps.push(psVector, array)

    assert(_vectorOps.nnz(psVector) == 2)
  }

  test("doAdd") {
    val constNum = 3.14
    _vectorOps.add(uniformVector, constNum, psVector)

    val result = uniformVector.pull().map(_ + constNum)
    assert(psVector.pull().sameElements(result))
  }

  test("doMul") {
    val constNum = 3.14
    _vectorOps.mul(uniformVector, constNum, psVector)

    val result = uniformVector.pull().map(_ * constNum)
    assert(psVector.pull().sameElements(result))
  }

  test("doDiv") {
    val constNum = 3.14
    _vectorOps.div(uniformVector, constNum, psVector)

    val result = uniformVector.pull().map(_ / constNum)
    assert(psVector.pull().sameElements(result))
  }

  test("doPow") {
    val constNum = 3.14
    _vectorOps.pow(uniformVector, constNum, psVector)

    val result = uniformVector.pull().map(SMath.pow(_, constNum))
    assert(psVector.pull().sameElements(result))
  }

  test("doSqrt") {
    _vectorOps.sqrt(uniformVector, psVector)

    val result = uniformVector.pull().map(x => SMath.sqrt(x))
    assert(psVector.pull().sameElements(result))
  }

  test("doExp") {
    _vectorOps.exp(uniformVector, psVector)

    val result = uniformVector.pull().map(x => SMath.exp(x))
    assert(psVector.pull().sameElements(result))
  }

  test("doExpm1") {
    _vectorOps.expm1(uniformVector, psVector)

    val result = uniformVector.pull().map(x => SMath.expm1(x))
    assert(psVector.pull().sameElements(result))
  }

  test("doLog") {
    _vectorOps.log(uniformVector, psVector)

    val result = uniformVector.pull().map(x => SMath.log(x))
    assert(psVector.pull().sameElements(result))
  }

  test("doLog1p") {
    _vectorOps.log1p(uniformVector, psVector)

    val result = uniformVector.pull().map(x => SMath.log1p(x))
    assert(psVector.pull().sameElements(result))
  }

  test("doLog10") {
    _vectorOps.log10(uniformVector, psVector)

    val result = uniformVector.pull().map(x => SMath.log10(x))
    assert(psVector.pull().sameElements(result))
  }

  test("doCeil") {
    _vectorOps.ceil(uniformVector, psVector)

    val result = uniformVector.pull().map(x => SMath.ceil(x))
    assert(psVector.pull().sameElements(result))
  }

  test("doFloor") {
    _vectorOps.floor(uniformVector, psVector)

    val result = uniformVector.pull().map(x => SMath.floor(x))
    assert(psVector.pull().sameElements(result))
  }

  test("doRound") {
    _vectorOps.round(uniformVector, psVector)

    val result = uniformVector.pull().map(x => SMath.round(x))
    assert(psVector.pull().sameElements(result))
  }

  test("doAbs") {
    _vectorOps.abs(uniformVector, psVector)

    val result = uniformVector.pull().map(x => SMath.abs(x))
    assert(psVector.pull().sameElements(result))

    // sparse vector test
    val sVector = SparsePSVector(dim)
    val absVector = PSVector.duplicate(sVector)
    val fillPair = Array[(Long, Double)]((1L, 0.1), (2L, -0.2))
    sVector.fill(fillPair)

    _vectorOps.abs(sVector, absVector)

    val pullPair = absVector.sparsePull().sortBy(_._1)
    assert(pullPair.sameElements(fillPair.map(x=>(x._1, Math.abs(x._2)))))
  }

  test("doSignum") {
    _vectorOps.signum(uniformVector, psVector)

    val result = uniformVector.pull().map(x => SMath.signum(x))
    assert(psVector.pull().sameElements(result))
  }

  test("doAdd two PSVectors") {
    _vectorOps.add(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.pull()
    val normalArray = normalVector.pull()
    val result = (0 until dim).toArray.map(i => uniArray(i) + normalArray(i))
    assert(psVector.pull().sameElements(result))
  }

  test("doSub two PSVectors") {
    _vectorOps.sub(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.pull()
    val normalArray = normalVector.pull()
    val result = (0 until dim).toArray.map(i => uniArray(i) - normalArray(i))
    assert(psVector.pull().sameElements(result))
  }

  test("doMul two PSVectors") {
    _vectorOps.mul(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.pull()
    val normalArray = normalVector.pull()
    val result = (0 until dim).toArray.map(i => uniArray(i) * normalArray(i))
    assert(psVector.pull().sameElements(result))
  }

  test("doDiv two PSVectors") {
    _vectorOps.div(normalVector, uniformVector, psVector)

    val normalArray = normalVector.pull()
    val uniArray = uniformVector.pull()
    val result = (0 until dim).toArray.map(i => normalArray(i) / uniArray(i))
    assert(psVector.pull().sameElements(result))
  }

  test("doMax two PSVectors") {
    _vectorOps.max(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.pull()
    val normalArray = normalVector.pull()
    val result = (0 until dim).toArray.map (i =>
      if (uniArray(i) > normalArray(i)) uniArray(i) else normalArray(i)
    )
    assert(psVector.pull().sameElements(result))
  }

  test("doMin two PSVectors") {
    _vectorOps.min(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.pull()
    val normalArray = normalVector.pull()
    val result = (0 until dim).toArray.map (i =>
      if (uniArray(i) < normalArray(i)) uniArray(i) else normalArray(i)
    )
    assert(psVector.pull().sameElements(result))
  }

  test("doMap") {
    val multiplier = 2.0
    _vectorOps.map(normalVector, new MapFuncTest(multiplier), psVector)

    val result = normalVector.pull().map(x => multiplier * x * x)
    assert(psVector.pull().sameElements(result))
  }

  test("doZip2Map") {
    val multiplier = 2.0
    _vectorOps.zip2Map(uniformVector, normalVector, new Zip2MapFuncTest(multiplier), psVector)

    val uniformArray = uniformVector.pull()
    val normalArray = normalVector.pull()
    val result = (0 until dim).toArray.map { i =>
      multiplier * uniformArray(i) + normalArray(i) * normalArray(i)
    }

    assert(psVector.pull().sameElements(result))
  }

  test("doZip3Map") {
    val multiplier = 2.0
    _vectorOps.zip3Map(uniformVector, normalVector, normalVector, new Zip3MapFuncTest(multiplier), psVector)

    val uniformArray = uniformVector.pull()
    val normalArray = normalVector.pull()
    val result = (0 until dim).toArray.map { i =>
      multiplier * uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)
    }

    assert(psVector.pull().sameElements(result))
  }

  test("doMapWithIndex") {
    val multiplier = 2.0
    _vectorOps.mapWithIndex(normalVector, new MapWithIndexFuncTest(multiplier), psVector)

    val normalArray = normalVector.pull()
    val result = (0 until dim).toArray.map { i =>
      if (i == 0) {
        normalArray(i) * normalArray(i)
      } else {
        multiplier * normalArray(i) * normalArray(i)
      }
    }

    assert(psVector.pull().sameElements(result))
  }

  test("doZip2MapWithIndex") {
    val multiplier = 2.0
    _vectorOps.zip2MapWithIndex(uniformVector, normalVector, new Zip2MapWithIndexFuncTest(multiplier), psVector)

    val uniformArray = uniformVector.pull()
    val normalArray = normalVector.pull()
    val result = (0 until dim).toArray.map {i =>
      if (i == 0) {
        uniformArray(i) + normalArray(i) * normalArray(i)
      } else {
        multiplier * uniformArray(i) + normalArray(i) * normalArray(i)
      }
    }

    assert(psVector.pull().sameElements(result))
  }

  test("doZip3MapWithIndex") {
    val multiplier = 2.0
    _vectorOps.zip3MapWithIndex(uniformVector, normalVector, normalVector, new Zip3MapWithIndexFuncTest(multiplier), psVector)

    val uniformArray = uniformVector.pull()
    val normalArray = normalVector.pull()
    val result = (0 until dim).toArray.map { i =>
      if (i == 0) {
        uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)
      } else {
        multiplier * uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)

      }
    }

    assert(psVector.pull().sameElements(result))
  }

  test("doAxpy") {
    val uniformArray = uniformVector.pull()
    val normalArray = normalVector.pull()
    val result = (0 until dim).toArray.map { i =>
      normalArray(i) + 2.0 * uniformArray(i)
    }

    _vectorOps.axpy(2.0, uniformVector, normalVector)

    (0 until dim).foreach { i =>
      assert(normalVector.pull()(i) === result(i))
    }
  }

  test("doDot") {
    val dot = _vectorOps.dot(uniformVector, normalVector)
    val uniformArray = uniformVector.pull()
    val normalArray = normalVector.pull()
    val result = (0 until dim).toArray.map { i =>
      normalArray(i) * uniformArray(i)
    }.sum
    assert(dot === result)
  }

  test("doCopy") {
    _vectorOps.copy(normalVector, psVector)

    assert(psVector.pull().sameElements(normalVector.pull()))
  }

  test("doScal") {
    val result = normalVector.pull().map(_ * -0.1)
    _vectorOps.scal(-0.1, normalVector)
    val scale = normalVector.pull()

    (0 until dim).foreach { i =>
      assert(scale(i) === result(i))
    }
  }

  test("doNrm2") {
    val norm = _vectorOps.nrm2(normalVector)
    val result = SMath.sqrt(normalVector.pull().map(x => x * x).sum)
    assert(result === norm)
  }

  test("doAsum") {
    val asum = _vectorOps.asum(normalVector)
    val result = normalVector.pull().map(SMath.abs).sum
    assert(result === asum)
  }

  test("doAmax") {
    val amax = _vectorOps.amax(normalVector)
    val result = normalVector.pull().map(SMath.abs).max
    assert(result === amax)
  }

  test("doAmin") {
    val result = normalVector.pull().map(SMath.abs).min
    val amin = _vectorOps.amin(normalVector)

    assert(result === amin)
  }

}
