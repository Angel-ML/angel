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

package com.tencent.angel.spark.models.vector.enhanced

import it.unimi.dsi.fastutil.longs.LongOpenHashSet

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.{BLAS, DenseVector, SparseVector}
import com.tencent.angel.spark.models.vector.{DensePSVector, PSVector}
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class BreezePSVectorSuite extends PSFunSuite with SharedPSContext {

  private val dim = 10
  private val capacity = 10
  private var _psContext: PSContext = _
  private var _psVector: DensePSVector = _
  private var _brzVector1: BreezePSVector = _
  private var _brzVector2: BreezePSVector = _
  private var _brzVector3: BreezePSVector = _
  private var _localVector1: DenseVector = _
  private var _localVector2: DenseVector = _
  private var _localVector3: DenseVector = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _psContext = PSContext.instance()
    _psVector = PSVector.dense(dim, capacity)
  }

  override def afterAll(): Unit = {
    _psContext.destroyVectorPool(_psVector)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    _brzVector1 = PSVector.duplicate(_psVector).toBreeze
    _localVector1 = _brzVector1.pull.asInstanceOf[DenseVector]
    _brzVector2 = PSVector.duplicate(_psVector).randomUniform(1.0, 2.0).toBreeze
    _localVector2 = _brzVector2.pull.asInstanceOf[DenseVector]
    _brzVector3 = PSVector.duplicate(_psVector).randomNormal(0.0, 1.0).toBreeze
    _localVector3 = _brzVector3.pull.asInstanceOf[DenseVector]
  }

  override def afterEach(): Unit = {
    _brzVector1 = null
    _brzVector2 = null
    _brzVector3 = null
    _localVector1 = null
    _localVector2 = null
    _localVector3 = null
    super.afterEach()
  }

  def assertSameElement(brzVector: BreezePSVector, local: Array[Double]): Unit = {
    brzVector.pull match {
      case dv: DenseVector => assert(dv.values.sameElements(local))
      case _ => assert(false)
    }
  }

  test("canCreateZerosLike") {
    val zeroBrz = BreezePSVector.canCreateZerosLike(_brzVector2)

    assert(zeroBrz.pull.asInstanceOf[DenseVector].values.sameElements(Array.ofDim[Double](dim)))
    zeroBrz.delete()
  }

  test("canCopyBreezePSVector") {
    val temp = _brzVector1.copy

    assert(temp.id != _brzVector1.id)
    assertSameElement(temp, _localVector1.values)
  }

  test("canSetInto") {
    _brzVector1 := _brzVector2

    assertSameElement(_brzVector1, _localVector2.values)
  }

  test("canSetIntoS") {
    _brzVector1 := 0.11

    assertSameElement(_brzVector1,  Array.fill(dim)(0.11))
  }

  test("canAxpy") {
    breeze.linalg.axpy(0.1, _brzVector2, _brzVector1)

    val result = _localVector1.values.indices.map(i => _localVector1(i) + 0.1 * _localVector2(i))
    assertSameElement(_brzVector1, result.toArray)
  }

  /** Add **/
  test("canAddInto") {
    _brzVector2 += _brzVector3

    val sum = _localVector2.values.indices.map(i => _localVector2(i) + _localVector3(i))

    assertSameElement(_brzVector2, sum.toArray)
  }

  test("canAdd") {
    _brzVector1 = _brzVector2 + _brzVector3

    val sum = _localVector2.values.indices.map(i => _localVector2(i) + _localVector3(i))

    assertSameElement(_brzVector1, sum.toArray)
  }

  test("canAddIntoS") {
    _brzVector2 += 1.0

    val sum = _localVector2.values.map(_ + 1.0)
    assertSameElement(_brzVector2, sum)
  }

  test("canAddS") {
    _brzVector1 = _brzVector2 + 1.0

    val sum = _localVector2.values.map(_ + 1.0)
    assertSameElement(_brzVector1, sum)
  }

  /** Sub **/
  test("canSubInto") {
    _brzVector2 -= _brzVector3

    val result = _localVector2.values.indices.map(i => _localVector2(i) - _localVector3(i))
    assertSameElement(_brzVector2, result.toArray)
  }

  test("canSub") {
    _brzVector1 = _brzVector2 - _brzVector3

    val result = _localVector2.values.indices.map(i => _localVector2(i) - _localVector3(i))
    assertSameElement(_brzVector1, result.toArray)
  }

  test("canSubIntoS") {
    _brzVector2 -= 1.0

    val result = _localVector2.values.map(_ - 1.0)
    assertSameElement(_brzVector2, result)
  }

  test("canSubS") {
    _brzVector1 = _brzVector2 - 1.0

    val result = _localVector2.values.map(_ - 1.0)
    assertSameElement(_brzVector1, result)
  }

  /** Mul **/
  test("canMulInto") {
    _brzVector2 *= _brzVector3

    val result = _localVector2.values.indices.map(i => _localVector2(i) * _localVector3(i))
    assertSameElement(_brzVector2, result.toArray)
  }

  test("canMul") {
    _brzVector1 = _brzVector2 :* _brzVector3

    val result = _localVector2.values.indices.map(i => _localVector2(i) * _localVector3(i))
    assertSameElement(_brzVector1, result.toArray)
  }

  test("canMulIntoS") {
    _brzVector2 *= 0.2

    val result = _localVector2.values.map(_ * 0.2)
    assertSameElement(_brzVector2, result)
  }

  test("canMulS") {
    _brzVector1 = _brzVector2 :* 0.2

    val result = _localVector2.values.map(_ * 0.2)
    assertSameElement(_brzVector1, result)
  }

  test("negFromScale") {
    _brzVector1 = -_brzVector2

    val result = _localVector2.values.map(_ * -1)
    assertSameElement(_brzVector1, result)
  }

  /** Div **/
  test("canDivInto") {
    _brzVector2 /= _brzVector3

    val result = _localVector2.values.indices.map(i => _localVector2(i) / _localVector3(i))
    assertSameElement(_brzVector2, result.toArray)
  }

  test("canDiv") {
    _brzVector1 = _brzVector2 :/ _brzVector3

    val result = _localVector2.values.indices.map(i => _localVector2(i) / _localVector3(i))
    assertSameElement(_brzVector1, result.toArray)
  }

  test("canDivIntoS") {
    _brzVector2 /= 0.2

    val result = _localVector2.values.map(_ / 0.2)
    assertSameElement(_brzVector2, result.toArray)
  }

  test("canDivS") {
    _brzVector1 = _brzVector2 :/ 0.2

    val result = _localVector2.values.map(_ / 0.2)
    assertSameElement(_brzVector1, result)
  }

  test("canDot") {
    val psDot = _brzVector2 dot _brzVector3

    val localDot = _localVector2.values.indices.map(i => _localVector2(i) * _localVector3(i)).sum
    assert(psDot === localDot)
  }

  test("canNorm") {
    val psNorm = breeze.linalg.norm(_brzVector2)

    val localNorm = math.sqrt(_localVector2.values.map(x => x * x).sum)
    assert(psNorm === localNorm)
  }

  test("canNorm2") {
    val psNorm = breeze.linalg.norm(_brzVector2, 1)

    val localNorm = _localVector2.values.map(math.abs).sum
    assert(psNorm === localNorm)
  }

  test("canDim") {
    val thisDim = breeze.linalg.dim(_brzVector1)
    assert(dim === thisDim)
  }

  test("equals") {
    assert(_brzVector1 == _brzVector1)
    assert(_brzVector1 != _brzVector2)
    val copy = _brzVector2.copy
    assert(copy == _brzVector2)
  }

  /**
   * math operations
   * Only called on driver
   */
  import BreezePSVector.math
  test("math.max") {
    val brzMax = math.max(_brzVector2, _brzVector3)
    val max = (0 until dim).map(i => {
      if (_localVector2(i) > _localVector3(i)) _localVector2(i) else _localVector3(i)
    })

    assertSameElement(brzMax, max.toArray)
  }

  test("math.min") {
    val brzMin = math.min(_brzVector2, _brzVector3)
    val min = (0 until dim).map(i => {
      if (_localVector2(i) < _localVector3(i)) _localVector2(i) else _localVector3(i)
    })

    assertSameElement(brzMin, min.toArray)
  }

  test("math.pow") {
    val brzPow = math.pow(_brzVector2, 2)
    val pow = _localVector2.values.map(scala.math.pow(_, 2))

    assertSameElement(brzPow, pow)
  }

  test("math.sqrt") {
    val brzSqrt = math.sqrt(_brzVector2 :+ 1.0)
    val sqrt = _localVector2.values.map(x => scala.math.sqrt(x + 1.0))

    assertSameElement(brzSqrt, sqrt)
  }

  test("math.exp") {
    val brzExp = math.exp(_brzVector2)
    val exp = _localVector2.values.map(scala.math.exp)

    assertSameElement(brzExp, exp)
  }

  test("math.expm1") {
    val brzExpm1 = math.expm1(_brzVector2)
    val expm1 = _localVector2.values.map(scala.math.expm1)

    assertSameElement(brzExpm1, expm1)
  }

  test("math.log") {
    val brzLog = math.log(_brzVector2)
    val log = _localVector2.values.map(scala.math.log)

    assertSameElement(brzLog, log)
  }

  test("math.log1p") {
    val brzLog1p = math.log1p(_brzVector2)
    val log1p = _localVector2.values.map(scala.math.log1p)

    assertSameElement(brzLog1p, log1p)
  }

  test("math.log10") {
    val brzLog10 = math.log10(_brzVector2)
    val log10 = _localVector2.values.map(scala.math.log10)

    assertSameElement(brzLog10, log10)
  }

  test("math.ceil") {
    val brzCeil = math.ceil(_brzVector2)
    val ceil = _localVector2.values.map(scala.math.ceil)

    assertSameElement(brzCeil, ceil)
  }

  test("math.floor") {
    val brzFloor = math.floor(_brzVector2)
    val floor = _localVector2.values.map(scala.math.floor)

    assertSameElement(brzFloor, floor)
  }

  test("math.round") {
    val brzRound = math.round(_brzVector2)
    val round = _localVector2.values.map(scala.math.round)

    assertSameElement(brzRound, round.map(_.toDouble))
  }

  test("math.abs") {
    val brzAbs = math.abs(_brzVector2)
    val abs = _localVector2.values.map(scala.math.abs)

    assertSameElement(brzAbs, abs)
  }

  test("math.signum") {
    val brzSignum = math.signum(_brzVector2)
    val signum = _localVector2.values.map(scala.math.signum)

    assertSameElement(brzSignum, signum)
  }

  /** math in place operations **/
  test("math.maxInto") {
    math.maxInto(_brzVector2, _brzVector3)
    val max = (0 until dim).map(i => {
      if (_localVector2(i) > _localVector3(i)) _localVector2(i) else _localVector3(i)
    })

    assertSameElement(_brzVector2, max.toArray)
  }

  test("math.minInto") {
    math.minInto(_brzVector2, _brzVector3)
    val min = (0 until dim).map(i => {
      if (_localVector2(i) < _localVector3(i)) _localVector2(i) else _localVector3(i)
    })

    assertSameElement(_brzVector2, min.toArray)
  }

  test("math.powInto") {
    math.powInto(_brzVector2, 2)
    val pow = _localVector2.values.map(scala.math.pow(_, 2))

    assertSameElement(_brzVector2, pow)
  }

  test("math.sqrtInto") {
    math.sqrtInto(_brzVector2)
    val sqrt = _localVector2.values.map(scala.math.sqrt)

    assertSameElement(_brzVector2, sqrt)
  }

  test("math.expInto") {
    math.expInto(_brzVector2)
    val exp = _localVector2.values.map(scala.math.exp)

    assertSameElement(_brzVector2, exp)
  }

  test("math.expm1Into") {
    math.expm1Into(_brzVector2)
    val expm1 = _localVector2.values.map(scala.math.expm1)

    assertSameElement(_brzVector2, expm1)
  }

  test("math.logInto") {
    math.logInto(_brzVector2)
    val log = _localVector2.values.map(scala.math.log)

    assertSameElement(_brzVector2, log)
  }

  test("math.log1pInto") {
    math.log1pInto(_brzVector2)
    val log1p = _localVector2.values.map(scala.math.log1p)

    assertSameElement(_brzVector2, log1p)
  }

  test("math.log10Into") {
    math.log10Into(_brzVector2)
    val log10 = _localVector2.values.map(scala.math.log10)

    assertSameElement(_brzVector2, log10)
  }

  test("math.ceilInto") {
    math.ceilInto(_brzVector2)
    val ceil = _localVector2.values.map(scala.math.ceil)

    assertSameElement(_brzVector2, ceil)
  }

  test("math.floorInto") {
    math.floorInto(_brzVector2)
    val floor = _localVector2.values.map(scala.math.floor)

    assertSameElement(_brzVector2, floor)
  }

  test("math.roundInto") {
    math.roundInto(_brzVector2)
    val round = _localVector2.values.map(scala.math.round)

    assertSameElement(_brzVector2, round.map(_.toDouble))
  }

  test("math.absInto") {
    math.absInto(_brzVector2)
    val abs = _localVector2.values.map(scala.math.abs)

    assertSameElement(_brzVector2, abs)
  }

  test("math.signumInto") {
    math.signumInto(_brzVector2)
    val signum = _localVector2.values.map(scala.math.signum)

    assertSameElement(_brzVector2, signum)
  }

  import BreezePSVector.blas
  test("blas.axpy") {
    blas.axpy(0.1, _brzVector2, _brzVector1)

    val result = _localVector1.values.indices.map(i => _localVector1(i) + 0.1 * _localVector2(i))
    assertSameElement(_brzVector1, result.toArray)
  }

  test("blas.dot") {
    val psDot = blas.dot(_brzVector2, _brzVector3)

    val localDot = _localVector2.values.indices.map(i => _localVector2(i) * _localVector3(i)).sum
    assert(psDot === localDot)
  }

  test("blas.scale") {
    val scale = _brzVector2.copy
    blas.scal(0.15, scale)

    val localScale = _localVector2.values.map(0.15 * _)
    assertSameElement(scale, localScale)
  }

  test("blas.copy") {
    val dist: BreezePSVector = PSVector.duplicate(_psVector).toBreeze
    blas.copy(_brzVector2, dist)
    assert(dist.id != _brzVector2.id)
    assert(dist.poolId == _brzVector2.poolId)
    assertSameElement(dist, _localVector2.values)
    dist.delete()
  }

  test("blas.nrm2") {
    val psNorm = blas.nrm2(_brzVector2)
    val localNorm = scala.math.sqrt(_localVector2.values.map(x => x * x).sum)
    assert(psNorm === localNorm)
  }

  test("blas.asum") {
    val psSum = blas.asum(_brzVector2)
    val localSum = _localVector2.values.map(scala.math.abs).sum
    assert(psSum === localSum)
  }

  test("blas.amax") {
    val psMax = blas.amax(_brzVector2)
    assert(psMax === _localVector2.values.max)
  }

  test("blas.amin") {
    val psMin = blas.amin(_brzVector2)
    assert(psMin === _localVector2.values.min)
  }

}
