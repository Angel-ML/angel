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

import scala.util.Random

import it.unimi.dsi.fastutil.longs.{Long2DoubleOpenHashMap, LongOpenHashSet}

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.{BLAS, SparseVector}
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
  private var _localVector1: SparseVector = _
  private var _localVector2: SparseVector = _
  private var _localVector3: SparseVector = _

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
    _localVector1 = _brzVector1.pull.toSparse
    _brzVector2 = PSVector.duplicate(_psVector).toBreeze
    _brzVector2.toSparse.push(randomSparseVector(dim))
    _localVector2 = _brzVector2.pull.toSparse
    _brzVector3 = PSVector.duplicate(_psVector).toBreeze
    _brzVector3.toSparse.push(randomSparseVector(dim))
    _localVector3 = _brzVector3.pull.toSparse
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

  def randomSparseVector(dim: Long): SparseVector = {
    val data = new Long2DoubleOpenHashMap()

    val rand = new Random()
    (0 until (dim / 2).toInt).foreach { id =>
      data.put(rand.nextLong(), rand.nextDouble())
    }
    new SparseVector(dim, data)
  }

  def assertSameElement(brzVector1: BreezePSVector, brzVector2: BreezePSVector): Unit = {
    val local1 = brzVector1.pull
    val local2 = brzVector2.pull
    assert(local1.isInstanceOf[SparseVector])
    assert(local2.isInstanceOf[SparseVector])
    assertSameElement(local1.toSparse, local2.toSparse)
  }

  def assertSameElement(brzVector: BreezePSVector, local: SparseVector): Unit = {
    brzVector.pull match {
      case sv: SparseVector => assertSameElement(sv, local)
      case _ => assert(false)
    }
  }

  def assertSameElement(sv1: SparseVector, sv2: SparseVector): Unit = {
    val data1 = sv1.keyValues
    val data2 = sv2.keyValues

    assert(sv1.length == sv2.length)
    assert(data1.defaultReturnValue() === data2.defaultReturnValue())
    val keys = new LongOpenHashSet(data1.keySet())
    keys.addAll(data2.keySet())

    val iter = keys.iterator()
    while (iter.hasNext) {
      val key = iter.nextLong()
      assert(data1.get(key) === data2.get(key))
    }
  }

  def newSparseVector(dim: Long, value: Double): SparseVector = {
    val local = new Long2DoubleOpenHashMap()
    local.defaultReturnValue(value)
    new SparseVector(dim, local)
  }


  test("canCreateZerosLike") {
    val zeroBrz = BreezePSVector.canCreateZerosLike(_brzVector2)

    assertSameElement(zeroBrz, new SparseVector(dim))

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

  test("canSetIntoS") {
    _brzVector1 := 0.11

    assertSameElement(_brzVector1, newSparseVector(dim, 0.11))
  }

  test("canAxpy") {
    breeze.linalg.axpy(0.1, _brzVector2, _brzVector1)

    BLAS.axpy(0.1, _localVector2, _localVector1)

    assertSameElement(_brzVector1, _localVector1)
  }

  /** Add **/
  test("canAddInto") {
    _brzVector2 += _brzVector3

    BLAS.axpy(1.0, _localVector3, _localVector2)

    assertSameElement(_brzVector2, _localVector2)
  }


  test("canAdd") {
    _brzVector1 = _brzVector2 + _brzVector3

    BLAS.axpy(1.0, _localVector3, _localVector2)
    BLAS.axpy(1.0, _localVector2, _localVector1)

    assertSameElement(_brzVector1, _localVector1)
  }


  test("canAddIntoS") {
    _brzVector2 += 1.0

    BLAS.axpy(1.0, newSparseVector(dim, 1.0), _localVector2)

    assertSameElement(_brzVector2, _localVector2)
  }

  test("canAddS") {
    _brzVector1 = _brzVector2 + 1.0

    BLAS.axpy(1.0, newSparseVector(dim, 1.0), _localVector2)
    assertSameElement(_brzVector1, _localVector2)
  }

  /** Sub **/
  test("canSubInto") {
    _brzVector2 -= _brzVector3

    BLAS.axpy(-1.0, _localVector3, _localVector2)

    assertSameElement(_brzVector2, _localVector2)
  }

  test("canSub") {
    _brzVector1 = _brzVector2 - _brzVector3

    BLAS.axpy(-1.0, _localVector3, _localVector2)

    assertSameElement(_brzVector1, _localVector2)
  }

  test("canSubIntoS") {
    _brzVector2 -= 1.0

    BLAS.axpy(-1.0, newSparseVector(dim, 1.0), _localVector2)

    assertSameElement(_brzVector2, _localVector2)
  }

  test("canSubS") {
    _brzVector1 = _brzVector2 - 1.0

    BLAS.axpy(-1.0, newSparseVector(dim, 1.0), _localVector2)
    assertSameElement(_brzVector1, _localVector2)
  }
  
  /** Mul **/
  test("canMulInto") {
    _brzVector2 *= _brzVector3

    val result = elementWiseOps(_localVector2, _localVector3, (a, b) => a * b)
    assertSameElement(_brzVector2, result)
  }

  test("canMul") {
    _brzVector1 = _brzVector2 :* _brzVector3

    val result = elementWiseOps(_localVector2, _localVector3, (a, b) => a * b)
    assertSameElement(_brzVector1, result)
  }

  test("canMulIntoS") {
    _brzVector2 *= 0.2

    val result = elementWiseOps(_localVector2, a => a * 0.2)
    assertSameElement(_brzVector2, result)
  }

  test("canMulS") {
    _brzVector1 = _brzVector2 :* 0.2

    val result = elementWiseOps(_localVector2, a => a * 0.2)
    assertSameElement(_brzVector1, result)
  }

  test("negFromScale") {
    _brzVector1 = -_brzVector2

    val result = elementWiseOps(_localVector1, _localVector2, (a, b) => a - b)
    assertSameElement(_brzVector1, result)
  }

  /**
  /** Div **/
  test("canDivInto") {
    _brzVector2 /= _brzVector3

    val result = elementWiseOps(_localVector2, _localVector3, (a, b) => a / b)
    assertSameElement(_brzVector2, result)
  }

  test("canDiv") {
    _brzVector1 = _brzVector2 :/ _brzVector3

    val result = elementWiseOps(_localVector2, _localVector3, (a, b) => a / b)
    assertSameElement(_brzVector1, result)
  }
   */

  test("canDivIntoS") {
    _brzVector2 /= 0.2

    val result = elementWiseOps(_localVector2, a => a / 0.2)
    assertSameElement(_brzVector2, result)
  }

  test("canDivS") {
    _brzVector1 = _brzVector2 :/ 0.2

    val result = elementWiseOps(_localVector2, a => a / 0.2)
    assertSameElement(_brzVector1, result)
  }

  test("canDot") {
    val psDot = _brzVector2 dot _brzVector3

    val localDot = BLAS.dot(_localVector2, _localVector3)
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
    val max = compare(_localVector2, _localVector3, (a, b) => if(a > b) a else b)

    assertSameElement(brzMax, max)
  }

  test("math.min") {
    val brzMin = math.min(_brzVector2, _brzVector3)
    val min = compare(_localVector2, _localVector3, (a, b) => if(a < b) a else b)

    assertSameElement(brzMin, min)
  }

  test("math.pow") {
    val brzPow = math.pow(_brzVector2, 2)
    val pow = elementWiseOps(_localVector2, a => a * a)

    assertSameElement(brzPow, pow)
  }

  test("math.sqrt") {
    val brzSqrt = math.sqrt(_brzVector2 :+ 1.0)

    val temp = elementWiseOps(_localVector2, a => a + 1.0)
    val result = elementWiseOps(temp, scala.math.sqrt)

    assertSameElement(brzSqrt, result)
  }

  test("math.exp") {
    val brzExp = math.exp(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.exp)

    assertSameElement(brzExp, result)
  }

  test("math.expm1") {
    val brzExpm1 = math.expm1(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.expm1)

    assertSameElement(brzExpm1, result)
  }

  test("math.log") {
    val brzLog = math.log(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.log)

    assertSameElement(brzLog, result)
  }

  test("math.log1p") {
    val brzLog1p = math.log1p(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.log1p)

    assertSameElement(brzLog1p, result)
  }

  test("math.log10") {
    val brzLog10 = math.log10(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.log10)

    assertSameElement(brzLog10, result)
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
    val max = compare(_localVector2, _localVector3, (a, b) => if(a > b) a else b)

    assertSameElement(_brzVector2, max)
  }

  test("math.minInto") {
    math.minInto(_brzVector2, _brzVector3)
    val min = compare(_localVector2, _localVector3, (a, b) => if(a < b) a else b)

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
    math.expInto(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.exp)

    assertSameElement(_brzVector2, result)
  }

  test("math.expm1Into") {
    math.expm1Into(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.expm1)

    assertSameElement(_brzVector2, result)
  }

  test("math.logInto") {
    math.logInto(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.log)

    assertSameElement(_brzVector2, result)
  }

  test("math.log1pInto") {
    math.log1pInto(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.log1p)

    assertSameElement(_brzVector2, result)
  }

  test("math.log10Into") {
    math.log10Into(_brzVector2)
    val result = elementWiseOps(_localVector2, scala.math.log10)

    assertSameElement(_brzVector2, result)
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

    BLAS.axpy(0.1, _localVector2, _localVector1)

    assertSameElement(_brzVector1, _localVector1)
  }

  test("blas.dot") {
    val psDot = blas.dot(_brzVector2, _brzVector3)

    val localDot = BLAS.dot(_localVector2, _localVector3)

    assert(psDot === localDot)
  }

  test("blas.scale") {
    blas.scal(0.15, _brzVector2)

    BLAS.scal(0.15, _localVector2)
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

  def elementWiseOps(sv1: SparseVector, sv2: SparseVector, ops: (Double, Double) => Double): SparseVector = {
    assert(sv1.length == sv2.length)
    val keySet = new LongOpenHashSet(sv1.keyValues.keySet())
    keySet.addAll(sv2.keyValues.keySet())

    val result = new Long2DoubleOpenHashMap()
    val iter = keySet.iterator()
    while (iter.hasNext) {
      val key = iter.nextLong()
      result.put(key, ops(sv1(key), sv2(key)))
    }
    val default1 = sv1.keyValues.defaultReturnValue()
    val default2 = sv2.keyValues.defaultReturnValue()
    result.defaultReturnValue(ops(default1, default2))
    new SparseVector(sv1.length, result)
  }

  def elementWiseOps(sv: SparseVector, ops: Double => Double): SparseVector = {
    val result = sv.keyValues.clone()
    val iter = result.long2DoubleEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      entry.setValue(ops(entry.getDoubleValue))
    }
    val default1 = result.defaultReturnValue()
    result.defaultReturnValue(ops(default1))
    new SparseVector(sv.length, result)
  }

  def elementWiseAggr(sv: SparseVector, seq: Double => Double, comb: (Double, Double) => Double): Double = {
    val temp = elementWiseOps(sv, seq)
    comb(temp.keyValues.defaultReturnValue(), temp.values.reduce(comb))
  }

  def compare(sv1: SparseVector, sv2: SparseVector, comb: (Double, Double) => Double): SparseVector = {
    assert(sv1.length == sv2.length)
    val keySet = new LongOpenHashSet(sv1.keyValues.keySet())
    keySet.addAll(sv2.keyValues.keySet())

    val result = new Long2DoubleOpenHashMap()
    val iter = keySet.iterator()
    while (iter.hasNext) {
      val key = iter.nextLong()
      result.put(key, comb(sv1(key), sv2(key)))
    }
    val default1 = sv1.keyValues.defaultReturnValue()
    val default2 = sv2.keyValues.defaultReturnValue()
    result.defaultReturnValue(comb(default1, default2))
    new SparseVector(sv1.length, result)
  }

}
