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

package com.tencent.angel.spark.math.vector.decorator

import scala.util.Random

import org.apache.spark.rdd.RDD
import org.scalatest.Matchers

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.math.vector.{DensePSVector, PSVector}
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class RemotePSVectorSuite extends PSFunSuite with Matchers with SharedPSContext with Serializable {

  private val dim = 10
  private val capacity = 10
  private val rddCount = 11
  private val partitionNum = 3
  private var _psContext: PSContext = _
  private var _psVector: DensePSVector = _

  @transient private var _rdd: RDD[Array[Double]] = _
  @transient private var _localSum: Array[Double] = _
  @transient private var _localMax: Array[Double] = _
  @transient private var _localMin: Array[Double] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _psContext = PSContext.instance()

    val thisDim = dim
    _rdd = sc.parallelize(0 until rddCount, partitionNum).map {i =>
      val rand = new Random(42)
      (0 until thisDim).toArray.map(i => rand.nextGaussian())
    }
    _rdd.cache()
    _rdd.count()

    _localSum = new Array[Double](dim)
    _localMax = Array.fill[Double](dim)(Double.MinValue)
    _localMin = Array.fill[Double](dim)(Double.MaxValue)
    _rdd.collect().foreach { arr =>
      arr.indices.foreach { i =>
        _localSum(i) += arr(i)
        if (_localMax(i) < arr(i)) _localMax(i) = arr(i)
        if (_localMin(i) > arr(i)) _localMin(i) = arr(i)
      }
    }
    _psVector = PSVector.dense(dim, capacity)
  }

  override def afterAll(): Unit = {
    _rdd.unpersist()
    _rdd = null
    _psVector = null
    _psContext = null
    super.afterAll()
  }

  test("increment dense vector") {
    val remoteVector = PSVector.duplicate(_psVector).toRemote
    val rdd2 = _rdd.mapPartitions { iter =>
      iter.foreach { arr =>
        remoteVector.increment(arr)
      }
      remoteVector.flush()
      Iterator.empty
    }
    rdd2.count()

    val psArray = remoteVector.pull(false)
    _localSum.indices.foreach { i => assert(_localSum(i) === psArray(i) +- doubleEps) }
  }

  test("increment dense vector and flush") {
    val remoteVector = PSVector.duplicate(_psVector).toRemote
    val rdd2 = _rdd.mapPartitions { iter =>
      iter.foreach { arr =>
        remoteVector.increment(arr)
      }
      Iterator.empty
    }
    rdd2.count()
    remoteVector.flush()

    val psArray = remoteVector.pull()
    _localSum.indices.foreach { i => assert(_localSum(i) === psArray(i) +- doubleEps) }
  }

  test("incrementAndFlush") {
    val remoteVector = PSVector.duplicate(_psVector).toRemote
    val rdd2 = _rdd.mapPartitions { iter =>
      val sum = iter.reduce { (arr1: Array[Double], arr2: Array[Double]) =>
        arr1.indices.toArray.map (i => arr1(i) + arr2(i))
      }
      remoteVector.incrementAndFlush(sum)
      Iterator.empty
    }
    rdd2.count()

    val psArray = remoteVector.pull()
    _localSum.indices.foreach { i => assert(_localSum(i) === psArray(i) +- doubleEps) }
  }

  test("mergeMax dense Vector") {
    val remoteVector = PSVector.duplicate(_psVector).fill(Double.NegativeInfinity).toRemote

    val rdd2 = _rdd.mapPartitions { iter =>
      iter.foreach( arr => remoteVector.mergeMax(arr) )
      remoteVector.flush()
      Iterator.empty
    }
    rdd2.count()
    assert(remoteVector.pull().sameElements(_localMax))
  }


  test("mergeMaxAndFlush") {
    val remoteVector = PSVector.duplicate(_psVector).fill(Double.NegativeInfinity).toRemote

    val thisDim = dim
    val rdd2 = _rdd.mapPartitions { iter =>
      val max = Array.fill[Double](thisDim)(Double.MinValue)
      iter.foreach { arr =>
        arr.indices.foreach { i => if (arr(i) >  max(i)) max(i) = arr(i) }
      }
      remoteVector.mergeMaxAndFlush(max)
      Iterator.empty
    }
    rdd2.count()

    assert(remoteVector.pull().sameElements(_localMax))
  }

  test("mergeMin dense vector") {
    val remoteVector = PSVector.duplicate(_psVector).fill(Double.PositiveInfinity).toRemote

    val rdd2 = _rdd.mapPartitions { iter =>
      iter.foreach( arr => remoteVector.mergeMin(arr) )
      remoteVector.flush()
      Iterator.empty
    }
    rdd2.count()

    assert(remoteVector.pull().sameElements(_localMin))
  }

  test("mergeMinAndFlush") {
    val remoteVector = PSVector.duplicate(_psVector).fill(Double.PositiveInfinity).toRemote

    val thisDim = dim
    val rdd2 = _rdd.mapPartitions { iter =>
      val min = Array.fill[Double](thisDim)(Double.MaxValue)
      iter.foreach { arr =>
        arr.indices.foreach { i => if (arr(i) <  min(i)) min(i) = arr(i) }
      }
      remoteVector.mergeMinAndFlush(min)
      Iterator.empty
    }
    rdd2.count()

    assert(remoteVector.pull().sameElements(_localMin))
  }
}
