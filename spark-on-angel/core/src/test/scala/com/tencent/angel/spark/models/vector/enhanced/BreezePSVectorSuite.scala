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

import com.tencent.angel.spark.context.PSContext
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
  private var _localVector1: Array[Double] = _
  private var _localVector2: Array[Double] = _
  private var _localVector3: Array[Double] = _

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
    _localVector1 = _brzVector1.pull()
    _brzVector2 = PSVector.duplicate(_psVector).randomUniform(1.0, 2.0).toBreeze
    _localVector2 = _brzVector2.pull()
    _brzVector3 = PSVector.duplicate(_psVector).randomNormal(0.0, 1.0).toBreeze
    _localVector3 = _brzVector3.pull()
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

  test("canCreateZerosLike") {
    val zeroBrz = BreezePSVector.canCreateZerosLike(_brzVector2)

    assert(zeroBrz.pull().sameElements(Array.ofDim[Double](dim)))
    zeroBrz.delete()
  }

  test("canCopyBreezePSVector") {
    val temp = _brzVector1.copy

    assert(temp.id != _brzVector1.id)
    assert(temp.pull().sameElements(_localVector1))
  }

  test("canSetInto") {
    _brzVector1 := _brzVector2

    assert(_brzVector1.pull().sameElements(_localVector2))
  }

  test("canSetIntoS") {
    _brzVector1 := 0.11

    assert(_brzVector1.pull().sameElements(Array.fill(dim)(0.11)))
  }

  test("canAxpy") {
    breeze.linalg.axpy(0.1, _brzVector2, _brzVector1)

    val result = _localVector1.indices.map(i => _localVector1(i) + 0.1 * _localVector2(i))
    assert(_brzVector1.pull().sameElements(result))
  }

  /** Add **/
  test("canAddInto") {
    _brzVector2 += _brzVector3

    val sum = _localVector2.indices.map(i => _localVector2(i) + _localVector3(i))
    assert(_brzVector2.pull().sameElements(sum))
  }

  test("canAdd") {
    _brzVector1 = _brzVector2 + _brzVector3

    val sum = _localVector2.indices.map(i => _localVector2(i) + _localVector3(i))

    assert(_brzVector1.pull().sameElements(sum))
  }

  test("canAddIntoS") {
    _brzVector2 += 1.0

    val sum = _localVector2.map(_ + 1.0)
    assert(_brzVector2.pull().sameElements(sum))
  }

  test("canAddS") {
    _brzVector1 = _brzVector2 + 1.0

    val sum = _localVector2.map(_ + 1.0)
    assert(_brzVector1.pull().sameElements(sum))
  }

  /** Sub **/
  test("canSubInto") {
    _brzVector2 -= _brzVector3

    val result = _localVector2.indices.map(i => _localVector2(i) - _localVector3(i))
    assert(_brzVector2.pull().sameElements(result))
  }

  test("canSub") {
    _brzVector1 = _brzVector2 - _brzVector3

    val result = _localVector2.indices.map(i => _localVector2(i) - _localVector3(i))
    assert(_brzVector1.pull().sameElements(result))
  }

  test("canSubIntoS") {
    _brzVector2 -= 1.0

    val result = _localVector2.map(_ - 1.0)
    assert(_brzVector2.pull().sameElements(result))
  }

  test("canSubS") {
    _brzVector1 = _brzVector2 - 1.0

    val result = _localVector2.map(_ - 1.0)
    assert(_brzVector1.pull().sameElements(result))
  }

  /** Mul **/
  test("canMulInto") {
    _brzVector2 *= _brzVector3

    val result = _localVector2.indices.map(i => _localVector2(i) * _localVector3(i))
    assert(_brzVector2.pull().sameElements(result))
  }

  test("canMul") {
    _brzVector1 = _brzVector2 :* _brzVector3

    val result = _localVector2.indices.map(i => _localVector2(i) * _localVector3(i))
    assert(_brzVector1.pull().sameElements(result))
  }

  test("canMulIntoS") {
    _brzVector2 *= 0.2

    val result = _localVector2.map(_ * 0.2)
    assert(_brzVector2.pull().sameElements(result))
  }

  test("canMulS") {
    _brzVector1 = _brzVector2 :* 0.2

    val result = _localVector2.map(_ * 0.2)
    assert(_brzVector1.pull().sameElements(result))
  }

  test("negFromScale") {
    _brzVector1 = -_brzVector2

    val result = _localVector2.map(_ * -1)
    assert(_brzVector1.pull().sameElements(result))
  }

  /** Div **/
  test("canDivInto") {
    _brzVector2 /= _brzVector3

    val result = _localVector2.indices.map(i => _localVector2(i) / _localVector3(i))
    assert(_brzVector2.pull().sameElements(result))
  }

  test("canDiv") {
    _brzVector1 = _brzVector2 :/ _brzVector3

    val result = _localVector2.indices.map(i => _localVector2(i) / _localVector3(i))
    assert(_brzVector1.pull().sameElements(result))
  }

  test("canDivIntoS") {
    _brzVector2 /= 0.2

    val result = _localVector2.map(_ / 0.2)
    assert(_brzVector2.pull().sameElements(result))
  }

  test("canDivS") {
    _brzVector1 = _brzVector2 :/ 0.2

    val result = _localVector2.map(_ / 0.2)
    assert(_brzVector1.pull().sameElements(result))
  }

  test("canDot") {
    val psDot = _brzVector2 dot _brzVector3

    val localDot = _localVector2.indices.map(i => _localVector2(i) * _localVector3(i)).sum
    assert(psDot === localDot)
  }

  test("canNorm") {
    val psNorm = breeze.linalg.norm(_brzVector2)

    val localNorm = math.sqrt(_localVector2.map(x => x * x).sum)
    assert(psNorm === localNorm)
  }

  test("canNorm2") {
    val psNorm = breeze.linalg.norm(_brzVector2, 1)

    val localNorm = _localVector2.map(math.abs).sum
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

    assert(brzMax.pull().sameElements(max))
  }

  test("math.min") {
    val brzMin = math.min(_brzVector2, _brzVector3)
    val min = (0 until dim).map(i => {
      if (_localVector2(i) < _localVector3(i)) _localVector2(i) else _localVector3(i)
    })

    assert(brzMin.pull().sameElements(min))
  }

  test("math.pow") {
    val brzPow = math.pow(_brzVector2, 2)
    val pow = _localVector2.map(scala.math.pow(_, 2))

    assert(brzPow.pull().sameElements(pow))
  }

  test("math.sqrt") {
    val brzSqrt = math.sqrt(_brzVector2 :+ 1.0)
    val sqrt = _localVector2.map(x => scala.math.sqrt(x + 1.0))

    assert(brzSqrt.pull().sameElements(sqrt))
  }

  test("math.exp") {
    val brzExp = math.exp(_brzVector2)
    val exp = _localVector2.map(scala.math.exp)

    assert(brzExp.pull().sameElements(exp))
  }

  test("math.expm1") {
    val brzExpm1 = math.expm1(_brzVector2)
    val expm1 = _localVector2.map(scala.math.expm1)

    assert(brzExpm1.pull().sameElements(expm1))
  }

  test("math.log") {
    val brzLog = math.log(_brzVector2)
    val log = _localVector2.map(scala.math.log)

    assert(brzLog.pull().sameElements(log))
  }

  test("math.log1p") {
    val brzLog1p = math.log1p(_brzVector2)
    val log1p = _localVector2.map(scala.math.log1p)

    assert(brzLog1p.pull().sameElements(log1p))
  }

  test("math.log10") {
    val brzLog10 = math.log10(_brzVector2)
    val log10 = _localVector2.map(scala.math.log10)

    assert(brzLog10.pull().sameElements(log10))
  }

  test("math.ceil") {
    val brzCeil = math.ceil(_brzVector2)
    val ceil = _localVector2.map(scala.math.ceil)

    assert(brzCeil.pull().sameElements(ceil))
  }

  test("math.floor") {
    val brzFloor = math.floor(_brzVector2)
    val floor = _localVector2.map(scala.math.floor)

    assert(brzFloor.pull().sameElements(floor))
  }

  test("math.round") {
    val brzRound = math.round(_brzVector2)
    val round = _localVector2.map(scala.math.round)

    assert(brzRound.pull().sameElements(round))
  }

  test("math.abs") {
    val brzAbs = math.abs(_brzVector2)
    val abs = _localVector2.map(scala.math.abs)

    assert(brzAbs.pull().sameElements(abs))
  }

  test("math.signum") {
    val brzSignum = math.signum(_brzVector2)
    val signum = _localVector2.map(scala.math.signum)

    assert(brzSignum.pull().sameElements(signum))
  }

  /** math in place operations **/
  test("math.maxInto") {
    math.maxInto(_brzVector2, _brzVector3)
    val max = (0 until dim).map(i => {
      if (_localVector2(i) > _localVector3(i)) _localVector2(i) else _localVector3(i)
    })

    assert(_brzVector2.pull().sameElements(max))
  }

  test("math.minInto") {
    math.minInto(_brzVector2, _brzVector3)
    val min = (0 until dim).map(i => {
      if (_localVector2(i) < _localVector3(i)) _localVector2(i) else _localVector3(i)
    })

    assert(_brzVector2.pull().sameElements(min))
  }

  test("math.powInto") {
    math.powInto(_brzVector2, 2)
    val pow = _localVector2.map(scala.math.pow(_, 2))

    assert(_brzVector2.pull().sameElements(pow))
  }

  test("math.sqrtInto") {
    math.sqrtInto(_brzVector2)
    val sqrt = _localVector2.map(scala.math.sqrt)

    assert(_brzVector2.pull().sameElements(sqrt))
  }

  test("math.expInto") {
    math.expInto(_brzVector2)
    val exp = _localVector2.map(scala.math.exp)

    assert(_brzVector2.pull().sameElements(exp))
  }

  test("math.expm1Into") {
    math.expm1Into(_brzVector2)
    val expm1 = _localVector2.map(scala.math.expm1)

    assert(_brzVector2.pull().sameElements(expm1))
  }

  test("math.logInto") {
    math.logInto(_brzVector2)
    val log = _localVector2.map(scala.math.log)

    assert(_brzVector2.pull().sameElements(log))
  }

  test("math.log1pInto") {
    math.log1pInto(_brzVector2)
    val log1p = _localVector2.map(scala.math.log1p)

    assert(_brzVector2.pull().sameElements(log1p))
  }

  test("math.log10Into") {
    math.log10Into(_brzVector2)
    val log10 = _localVector2.map(scala.math.log10)

    assert(_brzVector2.pull().sameElements(log10))
  }

  test("math.ceilInto") {
    math.ceilInto(_brzVector2)
    val ceil = _localVector2.map(scala.math.ceil)

    assert(_brzVector2.pull().sameElements(ceil))
  }

  test("math.floorInto") {
    math.floorInto(_brzVector2)
    val floor = _localVector2.map(scala.math.floor)

    assert(_brzVector2.pull().sameElements(floor))
  }

  test("math.roundInto") {
    math.roundInto(_brzVector2)
    val round = _localVector2.map(scala.math.round)

    assert(_brzVector2.pull().sameElements(round))
  }

  test("math.absInto") {
    math.absInto(_brzVector2)
    val abs = _localVector2.map(scala.math.abs)

    assert(_brzVector2.pull().sameElements(abs))
  }

  test("math.signumInto") {
    math.signumInto(_brzVector2)
    val signum = _localVector2.map(scala.math.signum)

    assert(_brzVector2.pull().sameElements(signum))
  }

  import BreezePSVector.blas
  test("blas.axpy") {
    blas.axpy(0.1, _brzVector2, _brzVector1)

    val result = _localVector1.indices.map(i => _localVector1(i) + 0.1 * _localVector2(i))
    assert(_brzVector1.pull().sameElements(result))
  }

  test("blas.dot") {
    val psDot = blas.dot(_brzVector2, _brzVector3)

    val localDot = _localVector2.indices.map(i => _localVector2(i) * _localVector3(i)).sum
    assert(psDot === localDot)
  }

  test("blas.scale") {
    val scale = _brzVector2.copy
    blas.scal(0.15, scale)

    val localScale = _localVector2.map(0.15 * _)
    assert(scale.pull().sameElements(localScale))
  }

  test("blas.copy") {
    val dist: BreezePSVector = PSVector.duplicate(_psVector).toBreeze
    blas.copy(_brzVector2, dist)
    assert(dist.id != _brzVector2.id)
    assert(dist.poolId == _brzVector2.poolId)
    assert(dist.pull().sameElements(_localVector2))
    dist.delete()
  }

  test("blas.nrm2") {
    val psNorm = blas.nrm2(_brzVector2)
    val localNorm = scala.math.sqrt(_localVector2.map(x => x * x).sum)
    assert(psNorm === localNorm)
  }

  test("blas.asum") {
    val psSum = blas.asum(_brzVector2)
    val localSum = _localVector2.map(scala.math.abs).sum
    assert(psSum === localSum)
  }

  test("blas.amax") {
    val psMax = blas.amax(_brzVector2)
    assert(psMax === _localVector2.max)
  }

  test("blas.amin") {
    val psMin = blas.amin(_brzVector2)
    assert(psMin === _localVector2.min)
  }

}
