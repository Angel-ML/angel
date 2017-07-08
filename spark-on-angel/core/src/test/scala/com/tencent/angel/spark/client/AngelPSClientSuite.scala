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

import com.tencent.angel.ml.matrix.psf.update.enhance.zip3.Zip3MapWithIndexFunc
import com.tencent.angel.spark._
import com.tencent.angel.spark.context.AngelPSContext
import com.tencent.angel.spark.pof._
import com.tencent.angel.spark.models.{PSModelPool, PSModelProxy}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach

class AngelPSClientSuite extends PSFunSuite with BeforeAndAfterEach {
  private val dim = 14
  private val capacity = 12
  private var _angel: AngelPSClient = _
  private var _pool: PSModelPool = _
  var psProxy: PSModelProxy = _
  var zeroProxy: PSModelProxy = _
  var uniformProxy: PSModelProxy = _
  var normalProxy: PSModelProxy = _

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
    val context = PSContext.getOrCreate(spark.sparkContext)
    _angel = PSClient().asInstanceOf[AngelPSClient]

    // create pool
    _pool = context.createModelPool(dim, capacity)
  }

  override def afterAll(): Unit = {
    PSContext.getOrCreate().destroyModelPool(_pool)
    _pool = null
    AngelPSContext.stop()
    _angel = null
    SparkSession.builder().getOrCreate().stop()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    psProxy = _pool.createZero()
    zeroProxy = _pool.createZero()
    uniformProxy = _pool.createRandomUniform(0.0, 1.0)
    normalProxy = _pool.createRandomNormal(0.0, 1.0)
  }

  override def afterEach(): Unit = {
    _pool.delete(normalProxy)
    _pool.delete(uniformProxy)
    _pool.delete(zeroProxy)
    _pool.delete(psProxy)
    super.afterEach()
  }

  test("doPull") {
    assert(_angel.pull(normalProxy).sameElements(normalProxy.mkRemote().pull()))
  }

  test("doIncrement") {
    val localArray = Array.fill[Double](dim)(0.1)

    val oldPSArray = normalProxy.mkRemote().pull()
    val result = localArray.indices.map(i => oldPSArray(i) + localArray(i))

    _angel.increment(normalProxy, localArray)

    assert(normalProxy.mkRemote().pull().sameElements(result))
  }

  test("doMergeMax") {
    val localPSVector = normalProxy.mkRemote().pull()

    val localArray = Array.fill[Double](dim)(0.1)
    _angel.mergeMax(normalProxy, localArray)

    val max = localArray.indices.map { i =>
      if (localArray(i) > localPSVector(i)) localArray(i) else localPSVector(i)
    }

    assert(normalProxy.mkRemote().pull().sameElements(max))
  }

  test("doMergeMin") {
    val localPSVector = normalProxy.mkRemote().pull()

    val localArray = Array.fill[Double](dim)(0.1)
    _angel.mergeMin(normalProxy, localArray)

    val min = localArray.indices.map { i =>
      if (localArray(i) < localPSVector(i)) localArray(i) else localPSVector(i)
    }

    assert(normalProxy.mkRemote().pull().sameElements(min))
  }

  test("doPush") {
    val psProxy = _pool.createZero()
    val localArray = Array.fill[Double](dim)(3.14)
    _angel.push(psProxy, localArray)
    assert(psProxy.mkRemote().pull().sameElements(localArray))
  }

  test("doFill") {
    val psProxy = _pool.createRandomUniform(0.0, 1.0)

    _angel.fill(psProxy, 3.14)
    assert(psProxy.mkRemote().pull().sameElements(Array.fill[Double](dim)(3.14)))
  }

  test("doRandomUniform") {
    val psProxy = _pool.createZero()
    _angel.randomUniform(psProxy, -1.0, 1.0)
    psProxy.mkRemote().pull().foreach { x =>
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
    * val variety = localArray.map(x => math.pow(x - mean, 2.0)).sum / (localArray.length - 1)

    * val tol = 0.1
    * assert(math.abs(mean - 0.0) < tol)
    * assert(math.abs(math.sqrt(variety) - 1.0) < tol)

    * _angel.destroyVectorPool(pool)
    * }
   **/

  test("doSum") {
    assert(uniformProxy.mkRemote().pull().sum === _angel.sum(uniformProxy))
  }

  test("doMax") {
    assert(normalProxy.mkRemote().pull().max === _angel.max(normalProxy))
  }

  test("doMin") {
    assert(normalProxy.mkRemote().pull().min === _angel.min(normalProxy))
  }

  test("doNnz") {
    val array = Array.fill[Double](dim)(0.0)
    array.update(3, 1.0)
    array.update(0, 1.0)
    _angel.push(psProxy, array)

    assert(_angel.nnz(psProxy) == 2)
  }

  test("doAdd") {
    val constNum = 3.14
    _angel.add(uniformProxy, constNum, psProxy)

    val result = uniformProxy.mkRemote().pull().map(_ + constNum)
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doMul") {
    val constNum = 3.14
    _angel.mul(uniformProxy, constNum, psProxy)

    val result = uniformProxy.mkRemote().pull().map(_ * constNum)
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doDiv") {
    val constNum = 3.14
    _angel.div(uniformProxy, constNum, psProxy)

    val result = uniformProxy.mkRemote().pull().map(_ / constNum)
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doPow") {
    val constNum = 3.14
    _angel.pow(uniformProxy, constNum, psProxy)

    val result = uniformProxy.mkRemote().pull().map(math.pow(_, constNum))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doSqrt") {
    _angel.sqrt(uniformProxy, psProxy)

    val result = uniformProxy.mkRemote().pull().map(x => math.sqrt(x))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doExp") {
    _angel.exp(uniformProxy, psProxy)

    val result = uniformProxy.mkRemote().pull().map(x => math.exp(x))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doExpm1") {
    _angel.expm1(uniformProxy, psProxy)

    val result = uniformProxy.mkRemote().pull().map(x => math.expm1(x))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doLog") {
    _angel.log(uniformProxy, psProxy)

    val result = uniformProxy.mkRemote().pull().map(x => math.log(x))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doLog1p") {
    _angel.log1p(uniformProxy, psProxy)

    val result = uniformProxy.mkRemote().pull().map(x => math.log1p(x))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doLog10") {
    _angel.log10(uniformProxy, psProxy)

    val result = uniformProxy.mkRemote().pull().map(x => math.log10(x))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doCeil") {
    _angel.ceil(uniformProxy, psProxy)

    val result = uniformProxy.mkRemote().pull().map(x => math.ceil(x))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doFloor") {
    _angel.floor(uniformProxy, psProxy)

    val result = uniformProxy.mkRemote().pull().map(x => math.floor(x))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doRound") {
    _angel.round(uniformProxy, psProxy)

    val result = uniformProxy.mkRemote().pull().map(x => math.round(x))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doAbs") {
    _angel.abs(uniformProxy, psProxy)

    val result = uniformProxy.mkRemote().pull().map(x => math.abs(x))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doSignum") {
    _angel.signum(uniformProxy, psProxy)

    val result = uniformProxy.mkRemote().pull().map(x => math.signum(x))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doAdd two PSVectors") {
    _angel.add(uniformProxy, normalProxy, psProxy)

    val uniArray = uniformProxy.mkRemote().pull()
    val normalArray = normalProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map(i => uniArray(i) + normalArray(i))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doSub two PSVectors") {
    _angel.sub(uniformProxy, normalProxy, psProxy)

    val uniArray = uniformProxy.mkRemote().pull()
    val normalArray = normalProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map(i => uniArray(i) - normalArray(i))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doMul two PSVectors") {
    _angel.mul(uniformProxy, normalProxy, psProxy)

    val uniArray = uniformProxy.mkRemote().pull()
    val normalArray = normalProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map(i => uniArray(i) * normalArray(i))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doDiv two PSVectors") {
    _angel.div(normalProxy, uniformProxy, psProxy)

    val normalArray = normalProxy.mkRemote().pull()
    val uniArray = uniformProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map(i => normalArray(i) / uniArray(i))
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doMax two PSVectors") {
    _angel.max(uniformProxy, normalProxy, psProxy)

    val uniArray = uniformProxy.mkRemote().pull()
    val normalArray = normalProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map (i =>
      if (uniArray(i) > normalArray(i)) uniArray(i) else normalArray(i)
    )
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doMin two PSVectors") {
    _angel.min(uniformProxy, normalProxy, psProxy)

    val uniArray = uniformProxy.mkRemote().pull()
    val normalArray = normalProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map (i =>
      if (uniArray(i) < normalArray(i)) uniArray(i) else normalArray(i)
    )
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doMap") {
    val multiplier = 2.0
    _angel.map(normalProxy, new MapFuncTest(multiplier), psProxy)

    val result = normalProxy.mkRemote().pull().map(x => multiplier * x * x)
    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doZip2Map") {
    val multiplier = 2.0
    _angel.zip2Map(uniformProxy, normalProxy, new Zip2MapFuncTest(multiplier), psProxy)

    val uniformArray = uniformProxy.mkRemote().pull()
    val normalArray = normalProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map { i =>
      multiplier * uniformArray(i) + normalArray(i) * normalArray(i)
    }

    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doZip3Map") {
    val multiplier = 2.0
    _angel.zip3Map(uniformProxy, normalProxy, normalProxy, new Zip3MapFuncTest(multiplier), psProxy)

    val uniformArray = uniformProxy.mkRemote().pull()
    val normalArray = normalProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map { i =>
      multiplier * uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)
    }

    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doMapWithIndex") {
    val multiplier = 2.0
    _angel.mapWithIndex(normalProxy, new MapWithIndexFuncTest(multiplier), psProxy)

    val normalArray = normalProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map { i =>
      if (i == 0) {
        normalArray(i) * normalArray(i)
      } else {
        multiplier * normalArray(i) * normalArray(i)
      }
    }

    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doZip2MapWithIndex") {
    val multiplier = 2.0
    _angel.zip2MapWithIndex(uniformProxy, normalProxy, new Zip2MapWithIndexFuncTest(multiplier), psProxy)

    val uniformArray = uniformProxy.mkRemote().pull()
    val normalArray = normalProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map {i =>
      if (i == 0) {
        uniformArray(i) + normalArray(i) * normalArray(i)
      } else {
        multiplier * uniformArray(i) + normalArray(i) * normalArray(i)
      }
    }

    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doZip3MapWithIndex") {
    val multiplier = 2.0
    _angel.zip3MapWithIndex(uniformProxy, normalProxy, normalProxy, new Zip3MapWithIndexFuncTest(multiplier), psProxy)

    val uniformArray = uniformProxy.mkRemote().pull()
    val normalArray = normalProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map { i =>
      if (i == 0) {
        uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)
      } else {
        multiplier * uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)

      }
    }

    assert(psProxy.mkRemote().pull().sameElements(result))
  }

  test("doAxpy") {
    val uniformArray = uniformProxy.mkRemote().pull()
    val normalArray = normalProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map { i =>
      normalArray(i) + 2.0 * uniformArray(i)
    }

    _angel.axpy(2.0, uniformProxy, normalProxy)

    (0 until dim).foreach { i =>
      assert(normalProxy.mkRemote().pull()(i) === result(i))
    }
  }

  test("doDot") {
    val dot = _angel.dot(uniformProxy, normalProxy)
    val uniformArray = uniformProxy.mkRemote().pull()
    val normalArray = normalProxy.mkRemote().pull()
    val result = (0 until dim).toArray.map { i =>
      normalArray(i) * uniformArray(i)
    }.sum
    assert(dot === result)
  }

  test("doCopy") {
    _angel.copy(normalProxy, psProxy)

    assert(psProxy.mkRemote().pull().sameElements(normalProxy.mkRemote().pull()))
  }

  test("doScal") {
    val result = normalProxy.mkRemote().pull().map(_ * -0.1)
    _angel.scal(-0.1, normalProxy)
    val scale = normalProxy.mkRemote().pull()

    (0 until dim).foreach { i =>
      assert(scale(i) === result(i))
    }
  }

  test("doNrm2") {
    val norm = _angel.nrm2(normalProxy)
    val result = math.sqrt(normalProxy.mkRemote().pull().map(x => x * x).sum)
    assert(result === norm)
  }

  test("doAsum") {
    val asum = _angel.asum(normalProxy)
    val result = normalProxy.mkRemote().pull().map(math.abs).sum
    assert(result === asum)
  }

  test("doAmax") {
    val amax = _angel.amax(normalProxy)
    val result = normalProxy.mkRemote().pull().map(math.abs).max
    assert(result === amax)
  }

  test("doAmin") {
    val result = normalProxy.mkRemote().pull().map(math.abs).min
    val amin = _angel.amin(normalProxy)

    assert(result === amin)
  }

}
