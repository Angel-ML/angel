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
 *
 */

package com.tencent.angel.spark.client

import scala.{math => SMath}

import com.tencent.angel.spark._
import com.tencent.angel.spark.context.{PSContext, PSVectorPool}
import com.tencent.angel.spark.pof._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach

import com.tencent.angel.spark.math.vector.{DensePSVector, PSVector}

class AngelPSClientSuite extends PSFunSuite with BeforeAndAfterEach {
  private val dim = 14
  private val capacity = 12
  private var _angel: AngelPSClient = _
  private var _pool: PSVectorPool = _
  var psVector: DensePSVector = _
  var zeroVector: DensePSVector = _
  var uniformVector: DensePSVector = _
  var normalVector: DensePSVector = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Angel config
    val psConf = new SparkConf()
      .set("spark.ps.mode", "LOCAL")
      .set("spark.ps.jars", "None")
      .set("spark.ps.out.path", "file:///tmp/output")
      .set("spark.ps.model.path", "file:///tmp/model")
      .set("spark.ps.instances", "1")
      .set("spark.ps.cores", "1")

    // Spark config
    val builder = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .config(psConf)

    // start Spark
    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    // start Angel
    PSContext.getOrCreate(spark.sparkContext)
    _angel = PSClient.instance().asInstanceOf[AngelPSClient]

    // create pool
    psVector = PSVector.dense(dim, capacity)
  }

  override def afterAll(): Unit = {
    PSContext.instance().destroyVectorPool(psVector)
    _pool = null
    PSContext.stop()
    _angel = null
    SparkSession.builder().getOrCreate().stop()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    zeroVector = PSVector.duplicate(psVector)
    uniformVector = PSVector.duplicate(psVector).randomUniform(0.0, 1.0)
    normalVector = PSVector.duplicate(psVector).randomNormal(0.0, 1.0)
  }

  override def afterEach(): Unit = {
    _pool.delete(normalVector)
    _pool.delete(uniformVector)
    _pool.delete(zeroVector)
    _pool.delete(psVector)
    super.afterEach()
  }

  test("doPull") {
    assert(_angel.pull(normalVector).sameElements(normalVector.toRemote.pull()))
  }

  test("doIncrement") {
    val localArray = Array.fill[Double](dim)(0.1)

    val oldPSArray = normalVector.toRemote.pull()
    val result = localArray.indices.map(i => oldPSArray(i) + localArray(i))

    _angel.increment(normalVector, localArray)

    assert(normalVector.toRemote.pull().sameElements(result))
  }

  test("doMergeMax") {
    val localPSVector = normalVector.toRemote.pull()

    val localArray = Array.fill[Double](dim)(0.1)
    _angel.mergeMax(normalVector, localArray)

    val max = localArray.indices.map { i =>
      if (localArray(i) > localPSVector(i)) localArray(i) else localPSVector(i)
    }

    assert(normalVector.toRemote.pull().sameElements(max))
  }

  test("doMergeMin") {
    val localPSVector = normalVector.toRemote.pull()

    val localArray = Array.fill[Double](dim)(0.1)
    _angel.mergeMin(normalVector, localArray)

    val min = localArray.indices.map { i =>
      if (localArray(i) < localPSVector(i)) localArray(i) else localPSVector(i)
    }

    assert(normalVector.toRemote.pull().sameElements(min))
  }

  test("doPush") {
    val psProxy = PSVector.duplicate(psVector)
    val localArray = Array.fill[Double](dim)(3.14)
    _angel.push(psProxy, localArray)
    assert(psProxy.toRemote.pull().sameElements(localArray))
  }

  test("doFill") {
    val psProxy = PSVector.duplicate(psVector).randomUniform(0.0, 1.0)

    _angel.fill(psProxy, 3.14)
    assert(psProxy.toRemote.pull().sameElements(Array.fill[Double](dim)(3.14)))
  }

  test("doRandomUniform") {
    val psProxy = PSVector.duplicate(psVector).randomUniform(0.0, 1.0)
    _angel.randomUniform(psProxy, -1.0, 1.0)
    psProxy.toRemote.pull().foreach { x =>
      assert(x < 1.0 && x > -1.0)
    }
  }

  /** Todo: a bug in Angel.
    * test("doRandomNormal") {
    * val pool = _angel.createVectorPool(10000, 2)
    * val psKey = pool.initModel()
    * _angel.doRandomNormal(psKey, 0.0, 1.0)

    * val localArray = psKey.toLocal().get()

    * val mean = localArray.sum / localArray.length
    * val variety = localArray.map(x => SMath.pow(x - mean, 2.0)).sum / (localArray.length - 1)

    * val tol = 0.1
    * assert(SMath.abs(mean - 0.0) < tol)
    * assert(SMath.abs(SMath.sqrt(variety) - 1.0) < tol)

    * _angel.destroyVectorPool(pool)
    * }
   **/

  test("doSum") {
    assert(uniformVector.toRemote.pull().sum === _angel.sum(uniformVector))
  }

  test("doMax") {
    assert(normalVector.toRemote.pull().max === _angel.max(normalVector))
  }

  test("doMin") {
    assert(normalVector.toRemote.pull().min === _angel.min(normalVector))
  }

  test("doNnz") {
    val array = Array.fill[Double](dim)(0.0)
    array.update(3, 1.0)
    array.update(0, 1.0)
    _angel.push(psVector, array)

    assert(_angel.nnz(psVector) == 2)
  }

  test("doAdd") {
    val constNum = 3.14
    _angel.add(uniformVector, constNum, psVector)

    val result = uniformVector.toRemote.pull().map(_ + constNum)
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doMul") {
    val constNum = 3.14
    _angel.mul(uniformVector, constNum, psVector)

    val result = uniformVector.toRemote.pull().map(_ * constNum)
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doDiv") {
    val constNum = 3.14
    _angel.div(uniformVector, constNum, psVector)

    val result = uniformVector.toRemote.pull().map(_ / constNum)
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doPow") {
    val constNum = 3.14
    _angel.pow(uniformVector, constNum, psVector)

    val result = uniformVector.toRemote.pull().map(SMath.pow(_, constNum))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doSqrt") {
    _angel.sqrt(uniformVector, psVector)

    val result = uniformVector.toRemote.pull().map(x => SMath.sqrt(x))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doExp") {
    _angel.exp(uniformVector, psVector)

    val result = uniformVector.toRemote.pull().map(x => SMath.exp(x))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doExpm1") {
    _angel.expm1(uniformVector, psVector)

    val result = uniformVector.toRemote.pull().map(x => SMath.expm1(x))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doLog") {
    _angel.log(uniformVector, psVector)

    val result = uniformVector.toRemote.pull().map(x => SMath.log(x))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doLog1p") {
    _angel.log1p(uniformVector, psVector)

    val result = uniformVector.toRemote.pull().map(x => SMath.log1p(x))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doLog10") {
    _angel.log10(uniformVector, psVector)

    val result = uniformVector.toRemote.pull().map(x => SMath.log10(x))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doCeil") {
    _angel.ceil(uniformVector, psVector)

    val result = uniformVector.toRemote.pull().map(x => SMath.ceil(x))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doFloor") {
    _angel.floor(uniformVector, psVector)

    val result = uniformVector.toRemote.pull().map(x => SMath.floor(x))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doRound") {
    _angel.round(uniformVector, psVector)

    val result = uniformVector.toRemote.pull().map(x => SMath.round(x))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doAbs") {
    _angel.abs(uniformVector, psVector)

    val result = uniformVector.toRemote.pull().map(x => SMath.abs(x))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doSignum") {
    _angel.signum(uniformVector, psVector)

    val result = uniformVector.toRemote.pull().map(x => SMath.signum(x))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doAdd two PSVectors") {
    _angel.add(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.toRemote.pull()
    val normalArray = normalVector.toRemote.pull()
    val result = (0 until dim).toArray.map(i => uniArray(i) + normalArray(i))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doSub two PSVectors") {
    _angel.sub(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.toRemote.pull()
    val normalArray = normalVector.toRemote.pull()
    val result = (0 until dim).toArray.map(i => uniArray(i) - normalArray(i))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doMul two PSVectors") {
    _angel.mul(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.toRemote.pull()
    val normalArray = normalVector.toRemote.pull()
    val result = (0 until dim).toArray.map(i => uniArray(i) * normalArray(i))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doDiv two PSVectors") {
    _angel.div(normalVector, uniformVector, psVector)

    val normalArray = normalVector.toRemote.pull()
    val uniArray = uniformVector.toRemote.pull()
    val result = (0 until dim).toArray.map(i => normalArray(i) / uniArray(i))
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doMax two PSVectors") {
    _angel.max(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.toRemote.pull()
    val normalArray = normalVector.toRemote.pull()
    val result = (0 until dim).toArray.map (i =>
      if (uniArray(i) > normalArray(i)) uniArray(i) else normalArray(i)
    )
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doMin two PSVectors") {
    _angel.min(uniformVector, normalVector, psVector)

    val uniArray = uniformVector.toRemote.pull()
    val normalArray = normalVector.toRemote.pull()
    val result = (0 until dim).toArray.map (i =>
      if (uniArray(i) < normalArray(i)) uniArray(i) else normalArray(i)
    )
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doMap") {
    val multiplier = 2.0
    _angel.map(normalVector, new MapFuncTest(multiplier), psVector)

    val result = normalVector.toRemote.pull().map(x => multiplier * x * x)
    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doZip2Map") {
    val multiplier = 2.0
    _angel.zip2Map(uniformVector, normalVector, new Zip2MapFuncTest(multiplier), psVector)

    val uniformArray = uniformVector.toRemote.pull()
    val normalArray = normalVector.toRemote.pull()
    val result = (0 until dim).toArray.map { i =>
      multiplier * uniformArray(i) + normalArray(i) * normalArray(i)
    }

    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doZip3Map") {
    val multiplier = 2.0
    _angel.zip3Map(uniformVector, normalVector, normalVector, new Zip3MapFuncTest(multiplier), psVector)

    val uniformArray = uniformVector.toRemote.pull()
    val normalArray = normalVector.toRemote.pull()
    val result = (0 until dim).toArray.map { i =>
      multiplier * uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)
    }

    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doMapWithIndex") {
    val multiplier = 2.0
    _angel.mapWithIndex(normalVector, new MapWithIndexFuncTest(multiplier), psVector)

    val normalArray = normalVector.toRemote.pull()
    val result = (0 until dim).toArray.map { i =>
      if (i == 0) {
        normalArray(i) * normalArray(i)
      } else {
        multiplier * normalArray(i) * normalArray(i)
      }
    }

    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doZip2MapWithIndex") {
    val multiplier = 2.0
    _angel.zip2MapWithIndex(uniformVector, normalVector, new Zip2MapWithIndexFuncTest(multiplier), psVector)

    val uniformArray = uniformVector.toRemote.pull()
    val normalArray = normalVector.toRemote.pull()
    val result = (0 until dim).toArray.map {i =>
      if (i == 0) {
        uniformArray(i) + normalArray(i) * normalArray(i)
      } else {
        multiplier * uniformArray(i) + normalArray(i) * normalArray(i)
      }
    }

    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doZip3MapWithIndex") {
    val multiplier = 2.0
    _angel.zip3MapWithIndex(uniformVector, normalVector, normalVector, new Zip3MapWithIndexFuncTest(multiplier), psVector)

    val uniformArray = uniformVector.toRemote.pull()
    val normalArray = normalVector.toRemote.pull()
    val result = (0 until dim).toArray.map { i =>
      if (i == 0) {
        uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)
      } else {
        multiplier * uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)

      }
    }

    assert(psVector.toRemote.pull().sameElements(result))
  }

  test("doAxpy") {
    val uniformArray = uniformVector.toRemote.pull()
    val normalArray = normalVector.toRemote.pull()
    val result = (0 until dim).toArray.map { i =>
      normalArray(i) + 2.0 * uniformArray(i)
    }

    _angel.axpy(2.0, uniformVector, normalVector)

    (0 until dim).foreach { i =>
      assert(normalVector.toRemote.pull()(i) === result(i))
    }
  }

  test("doDot") {
    val dot = _angel.dot(uniformVector, normalVector)
    val uniformArray = uniformVector.toRemote.pull()
    val normalArray = normalVector.toRemote.pull()
    val result = (0 until dim).toArray.map { i =>
      normalArray(i) * uniformArray(i)
    }.sum
    assert(dot === result)
  }

  test("doCopy") {
    _angel.copy(normalVector, psVector)

    assert(psVector.toRemote.pull().sameElements(normalVector.toRemote.pull()))
  }

  test("doScal") {
    val result = normalVector.toRemote.pull().map(_ * -0.1)
    _angel.scal(-0.1, normalVector)
    val scale = normalVector.toRemote.pull()

    (0 until dim).foreach { i =>
      assert(scale(i) === result(i))
    }
  }

  test("doNrm2") {
    val norm = _angel.nrm2(normalVector)
    val result = SMath.sqrt(normalVector.toRemote.pull().map(x => x * x).sum)
    assert(result === norm)
  }

  test("doAsum") {
    val asum = _angel.asum(normalVector)
    val result = normalVector.toRemote.pull().map(SMath.abs).sum
    assert(result === asum)
  }

  test("doAmax") {
    val amax = _angel.amax(normalVector)
    val result = normalVector.toRemote.pull().map(SMath.abs).max
    assert(result === amax)
  }

  test("doAmin") {
    val result = normalVector.toRemote.pull().map(SMath.abs).min
    val amin = _angel.amin(normalVector)

    assert(result === amin)
  }

}
