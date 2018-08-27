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


package com.tencent.angel.spark.models.vector.enhanced

import scala.language.implicitConversions
import scala.util.Random

import org.scalactic.TolerantNumerics

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.vector.LongDoubleVector
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.CompatibleImplicit._
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}


class SparseBreezePSVectorSuite extends PSFunSuite with SharedPSContext {

  private val dim = 10
  private val capacity = 10
  private var _psContext: PSContext = _
  private var _psVector: SparsePSVector = _
  private var _brzVector1: BreezePSVector = _
  private var _brzVector2: BreezePSVector = _
  private var _brzVector3: BreezePSVector = _
  private var _localVector1: LongDoubleVector = _
  private var _localVector2: LongDoubleVector = _
  private var _localVector3: LongDoubleVector = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _psContext = PSContext.instance()
    _psVector = PSVector.sparse(dim, capacity)
  }

  override def afterAll(): Unit = {
    _psContext.destroyVectorPool(_psVector)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    _brzVector1 = PSVector.duplicate(_psVector).toBreeze
    _localVector1 = _brzVector1.pull().asInstanceOf[LongDoubleVector]
    _brzVector2 = PSVector.duplicate(_psVector).toBreeze
    _brzVector2.push(randomSparseVector(dim))
    _localVector2 = _brzVector2.pull().asInstanceOf[LongDoubleVector]
    _brzVector3 = PSVector.duplicate(_psVector).toBreeze
    _brzVector3.toSparse.push(randomSparseVector(dim))
    _localVector3 = _brzVector3.pull().asInstanceOf[LongDoubleVector]
  }

  def randomSparseVector(dim: Long): LongDoubleVector = {
    val rand = new Random()
    val validNum = (dim / 2).toInt
    val keys = new Array[Long](validNum)
    val values = new Array[Double](validNum)
    (0 until validNum).foreach { i =>
      keys(i) = math.abs(rand.nextLong()) % dim
      values(i) = rand.nextDouble()
    }
    VFactory.sparseLongKeyDoubleVector(dim, keys, values)
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

  def assertSameElement(brzVector1: BreezePSVector, brzVector2: BreezePSVector): Unit = {
    val local1 = brzVector1.pull().asInstanceOf[LongDoubleVector]
    val local2 = brzVector2.pull().asInstanceOf[LongDoubleVector]
    assertSameElement(local1, local2)
  }

  def assertSameElement(brzVector: BreezePSVector, local: LongDoubleVector): Unit = {
    brzVector.pull() match {
      case sv: LongDoubleVector => assertSameElement(sv, local)
      case _ => assert(false)
    }
  }

  def assertSameElement(sv1: LongDoubleVector, sv2: LongDoubleVector): Unit = {
    assert(sv1.getDim == sv2.getDim)
    val keys = (sv1.getStorage.getIndices ++ sv2.getStorage.getIndices).toSet
    for (key <- keys) {
      if (sv1.get(key).isNaN)
        assert(sv2.get(key).isNaN)
      else {
        if (sv1.get(key) != sv2.get(key))
          println(s"$key, ${sv1.get(key)}, ${sv2.get(key)}")
        assert(sv1.get(key) === sv2.get(key))
      }

    }
  }

  def newSparseVector(dim: Long, value: Double): LongDoubleVector = {
    VFactory.sparseLongKeyDoubleVector(dim, (0L until dim).toArray, Array.fill(dim.toInt)(value))
  }


  test("canCreateZerosLike") {
    val zeroBrz = BreezePSVector.canCreateZerosLike(_brzVector2)
    assertSameElement(zeroBrz, VFactory.sparseLongKeyDoubleVector(dim, Array(), Array()))
    zeroBrz.delete()
  }


  test("canCopyBreezePSVector") {
    val temp = _brzVector1.copy

    assert(temp.id != _brzVector1.id)
    assertSameElement(temp, _localVector1)
  }

  test("canSetInto") {
    _brzVector1 := _brzVector2

    assertSameElement(_brzVector1, _localVector2)
  }

  // not support add const to a sparse vector, throw a error
  test("canSetIntoS") {
    val thrown = intercept[Exception] {
      _brzVector1 := 0.11
      assertSameElement(_brzVector1, newSparseVector(dim, 0.11))
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("canAxpy") {
    breeze.linalg.axpy(0.1, _brzVector2, _brzVector1)

    Ufuncs.iaxpy(_localVector1, _localVector2, 0.1)

    assertSameElement(_brzVector1, _localVector1)
  }

  /** Add **/
  test("canAddInto") {
    _brzVector2 += _brzVector3

    Ufuncs.iaxpy(_localVector2, _localVector3, 1.0)
    assertSameElement(_brzVector2, _localVector2)
  }


  test("canAdd") {
    _brzVector1 = _brzVector2 + _brzVector3
    assertSameElement(_brzVector1, _localVector2 + _localVector3)
  }


  test("canAddIntoS") {
    val thrown = intercept[Exception] {
      _brzVector2 += 1.0
      _localVector2 += newSparseVector(dim, 1.0)
      assertSameElement(_brzVector2, _localVector2)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("canAddS") {
    val thrown = intercept[Exception] {
      _brzVector1 = _brzVector2 + 1.0
      _localVector1 = _localVector2 + newSparseVector(dim, 1.0)
      assertSameElement(_brzVector1, _localVector1)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  /** Sub **/
  test("canSubInto") {
    _brzVector2 -= _brzVector3
    _localVector2 -= _localVector3
    assertSameElement(_brzVector2, _localVector2)
  }

  test("canSub") {
    _brzVector1 = _brzVector2 - _brzVector3
    _localVector1 = _localVector2 - _localVector3
    assertSameElement(_brzVector1, _localVector1)
  }

  test("canSubIntoS") {
    val thrown = intercept[Exception] {
      _brzVector2 -= 1.0
      _localVector2 -= newSparseVector(dim, 1.0)
      assertSameElement(_brzVector2, _localVector2)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("canSubS") {
    val thrown = intercept[Exception] {
      _brzVector1 = _brzVector2 - 1.0
      _localVector1 = _localVector2 - newSparseVector(dim, 1.0)
      assertSameElement(_brzVector1, _localVector1)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  /** Mul **/
  test("canMulInto") {
    _brzVector2 *= _brzVector3
    _localVector2.imul(_localVector3)
    assertSameElement(_brzVector2, _localVector2)
  }

  test("canMul") {
    _brzVector1 = _brzVector2 :* _brzVector3
    _localVector1 = _localVector2 * _localVector3
    assertSameElement(_brzVector1, _localVector1)
  }

  test("canMulIntoS") {
    _brzVector2 *= 0.2
    _localVector2 *= newSparseVector(dim, 0.2)
    assertSameElement(_brzVector2, _localVector2)

  }

  test("canMulS") {
    _brzVector1 = _brzVector2 :* 0.2
    _localVector1 = _localVector2 * newSparseVector(dim, 0.2)
    assertSameElement(_brzVector1, _localVector1)

  }

  test("negFromScale") {
    _brzVector1 = -_brzVector2
    _localVector1 = _localVector2 * newSparseVector(dim, -1.0)
    assertSameElement(_brzVector1, _localVector1)
  }

  /** Div **/
  test("canDivInto") {
    _brzVector2 /= _brzVector3
    _localVector2 /= _localVector3
    assertSameElement(_brzVector2, _localVector2)
  }

  test("canDiv") {
    _brzVector1 = _brzVector2 :/ _brzVector3
    _localVector1 = _localVector2 / _localVector3
    assertSameElement(_brzVector1, _localVector1)
  }


  test("canDivIntoS") {
    _brzVector2 /= 0.2
    _localVector2 /= newSparseVector(dim, 0.2)
  }

  test("canDivS") {
    _brzVector1 = _brzVector2 :/ 0.2
    _localVector1 = _localVector2 / newSparseVector(dim, 0.2)
    assertSameElement(_brzVector1, _localVector1)
  }

  test("canDot") {
    val psDot = _brzVector2 dot _brzVector3
    TolerantNumerics.tolerantDoubleEquality(1e-8)
    assert(psDot === _localVector2.dot(_localVector3))
  }

  test("canNorm2") {
    val psNorm = breeze.linalg.norm(_brzVector2)
    val localNorm = math.sqrt(_localVector2 dot _localVector2)
    TolerantNumerics.tolerantDoubleEquality(1e-8)
    assert(psNorm === localNorm)
  }

  test("canNorm") {
    val psNorm = breeze.linalg.norm(_brzVector2, 1)
    val localNorm = Ufuncs.abs(_localVector2) dot newSparseVector(dim, 1.0)
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
    val max = compare(_localVector2, _localVector3, (a, b) => if (a > b) a else b)
    assertSameElement(brzMax, max)
  }

  test("math.min") {
    val brzMin = math.min(_brzVector2, _brzVector3)
    val min = compare(_localVector2, _localVector3, (a, b) => if (a < b) a else b)

    assertSameElement(brzMin, min)
  }

  test("math.pow") {
    val brzPow = math.pow(_brzVector2, 2)
    val pow = elementWiseOps(_localVector2, a => a * a)

    assertSameElement(brzPow, pow)
  }

  test("math.sqrt") {
    val brzSqrt = math.sqrt(_brzVector2)

    val temp = elementWiseOps(_localVector2, a => a)
    val result = elementWiseOps(temp, scala.math.sqrt)

    assertSameElement(brzSqrt, result)
  }

  test("math.exp") {
    val thrown = intercept[Exception] {
      val brzExp = math.exp(_brzVector2)
      val result = elementWiseOps(_localVector2, scala.math.exp)

      assertSameElement(brzExp, result)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("math.expm1") {
    val thrown = intercept[Exception] {
      val brzExpm1 = math.expm1(_brzVector2)
      val result = elementWiseOps(_localVector2, scala.math.expm1)

      assertSameElement(brzExpm1, result)
    }

    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("math.log") {
    val thrown = intercept[Exception] {
      val brzLog = math.log(_brzVector2)
      val result = elementWiseOps(_localVector2, scala.math.log)

      assertSameElement(brzLog, result)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("math.log1p") {
    val thrown = intercept[Exception] {
      val brzLog1p = math.log1p(_brzVector2)
      val result = elementWiseOps(_localVector2, scala.math.log1p)

      assertSameElement(brzLog1p, result)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("math.log10") {
    val thrown = intercept[Exception] {
      val brzLog10 = math.log10(_brzVector2)
      val result = elementWiseOps(_localVector2, scala.math.log10)

      assertSameElement(brzLog10, result)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("math.ceil") {
    val brzCeil = math.ceil(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.ceil)

    assertSameElement(brzCeil, result)
  }

  test("math.floor") {
    val brzFloor = math.floor(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.floor)

    assertSameElement(brzFloor, result)
  }

  test("math.round") {
    val brzRound = math.round(_brzVector2)
    val result = elementWiseOps(_localVector2, x => scala.math.round(x).toDouble)

    assertSameElement(brzRound, result)
  }

  test("math.abs") {
    val brzAbs = math.abs(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.abs)

    assertSameElement(brzAbs, result)
  }

  test("math.signum") {
    val brzSignum = math.signum(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.signum)
    assertSameElement(brzSignum, result)
  }

  /** math in place operations **/
  test("math.maxInto") {
    math.maxInto(_brzVector2, _brzVector3)
    val max = compare(_localVector2, _localVector3, (a, b) => if (a > b) a else b)

    assertSameElement(_brzVector2, max)
  }

  test("math.minInto") {
    math.minInto(_brzVector2, _brzVector3)
    val min = compare(_localVector2, _localVector3, (a, b) => if (a < b) a else b)

    assertSameElement(_brzVector2, min)
  }

  test("math.powInto") {
    math.powInto(_brzVector2, 2)
    val result = elementWiseOps(_localVector2, scala.math.pow(_, 2))

    assertSameElement(_brzVector2, result)
  }

  test("math.sqrtInto") {
    math.sqrtInto(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.sqrt)

    assertSameElement(_brzVector2, result)
  }

  test("math.expInto") {
    val thrown = intercept[Exception] {
      math.expInto(_brzVector2)
      val result = elementWiseOps(_localVector2, scala.math.exp)

      assertSameElement(_brzVector2, result)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("math.expm1Into") {
    val thrown = intercept[Exception] {
      math.expm1Into(_brzVector2)
      val result = elementWiseOps(_localVector2, scala.math.expm1)

      assertSameElement(_brzVector2, result)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("math.logInto") {
    val thrown = intercept[Exception] {
      math.logInto(_brzVector2)
      val result = elementWiseOps(_localVector2, scala.math.log)

      assertSameElement(_brzVector2, result)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("math.log1pInto") {
    val thrown = intercept[Exception] {
      math.log1pInto(_brzVector2)
      val result = elementWiseOps(_localVector2, scala.math.log1p)

      assertSameElement(_brzVector2, result)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("math.log10Into") {
    val thrown = intercept[Exception] {
      math.log10Into(_brzVector2)
      val result = elementWiseOps(_localVector2, scala.math.log10)

      assertSameElement(_brzVector2, result)
    }
    assert(thrown.getMessage === "not support for sparse ps vector")
  }

  test("math.ceilInto") {
    math.ceilInto(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.ceil)

    assertSameElement(_brzVector2, result)
  }

  test("math.floorInto") {
    math.floorInto(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.floor)

    assertSameElement(_brzVector2, result)
  }

  test("math.roundInto") {
    math.roundInto(_brzVector2)
    val result = elementWiseOps(_localVector2, x => scala.math.round(x).toDouble)

    assertSameElement(_brzVector2, result)
  }

  test("math.absInto") {
    math.absInto(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.abs)

    assertSameElement(_brzVector2, result)
  }

  test("math.signumInto") {
    math.signumInto(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.signum)

    assertSameElement(_brzVector2, result)
  }

  import BreezePSVector.blas

  test("blas.axpy") {
    blas.axpy(0.1, _brzVector2, _brzVector1)
    Ufuncs.iaxpy(_localVector1, _localVector2, 0.1)

    assertSameElement(_brzVector1, _localVector1)
  }

  test("blas.dot") {
    val psDot = blas.dot(_brzVector2, _brzVector3)
    val localDot = Ufuncs.dot(_localVector2, _localVector3)

    assert(psDot === localDot)
  }

  test("blas.scale") {
    blas.scal(0.15, _brzVector2)
    Ufuncs.ismul(_localVector2, 0.15)
    assertSameElement(_brzVector2, _localVector2)
  }

  test("blas.copy") {
    val dist: BreezePSVector = PSVector.duplicate(_psVector).toBreeze
    blas.copy(_brzVector2, dist)
    assert(dist.id != _brzVector2.id)
    assert(dist.poolId == _brzVector2.poolId)
    dist.delete()
  }

  test("blas.nrm2") {
    val psNorm = blas.nrm2(_brzVector2)
    val localNorm = scala.math.sqrt(elementWiseAggr(_localVector2, x => x * x, (a, b) => a + b))
    assert(psNorm === localNorm)
  }

  test("blas.asum") {
    val psSum = blas.asum(_brzVector2)
    val localSum = elementWiseAggr(_localVector2, scala.math.abs, (a, b) => a + b)
    assert(psSum === localSum)
  }

  test("blas.amax") {
    val psMax = blas.amax(_brzVector2)
    val localMax = elementWiseAggr(_localVector2, scala.math.abs, (a, b) => if (a > b) a else b)
    assert(psMax === localMax)
  }

  test("blas.amin") {
    val psMin = blas.amin(_brzVector2)
    val localMin = elementWiseAggr(_localVector2, scala.math.abs, (a, b) => if (a < b) a else b)
    assert(psMin === localMin)
  }

  def elementWiseOps(sv1: LongDoubleVector,
      sv2: LongDoubleVector,
      comb: (Double, Double) => Double): LongDoubleVector = {
    assert(sv1.getDim == sv2.getDim)
    val keySet = (sv1.getStorage.getIndices.toSet ++ sv2.getStorage.getIndices).toArray
    VFactory.sparseLongKeyDoubleVector(sv1.getDim, keySet, keySet.map(k => comb(sv1.get(k), sv2.get(k))))
  }

  def elementWiseAggr(sv: LongDoubleVector, seq: Double => Double, comb: (Double, Double) => Double): Double = {
    val temp = elementWiseOps(sv, seq)
    temp.getStorage.getValues.reduce(comb)
  }

  def elementWiseOps(sv: LongDoubleVector, ops: Double => Double): LongDoubleVector = {
    val dim = sv.getDim
    val keys = (0L until dim).toArray
    VFactory.sparseLongKeyDoubleVector(dim, keys, keys.map(k => ops(sv.get(k))))
  }

  def compare(sv1: LongDoubleVector, sv2: LongDoubleVector, comb: (Double, Double) => Double): LongDoubleVector = {
    assert(sv1.getDim == sv2.getDim)
    val keySet = (sv1.getStorage.getIndices.toSet ++ sv2.getStorage.getIndices).toArray
    VFactory.sparseLongKeyDoubleVector(sv1.getDim, keySet, keySet.map(k => comb(sv1.get(k), sv2.get(k))))
  }
}