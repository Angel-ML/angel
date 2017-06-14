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

import java.util.concurrent.ConcurrentHashMap

import com.tencent.angel.ml.math.vector.DenseDoubleVector
import com.tencent.angel.ml.matrix.psf.aggr._
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult
import com.tencent.angel.ml.matrix.psf.aggr.primitive.Pull
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult
import com.tencent.angel.ml.matrix.psf.update._
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc
import com.tencent.angel.ml.matrix.psf.update.enhance.map.{Map, MapFunc, MapWithIndex, MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.{Zip2Map, Zip2MapFunc, Zip2MapWithIndex, Zip2MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.enhance.zip3.{Zip3Map, Zip3MapFunc, Zip3MapWithIndex, Zip3MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.primitive.{Increment, Push}
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark._
import com.tencent.angel.spark.models.PSModelProxy
import com.tencent.angel.spark.models.vector.{PSVector, RemotePSVector}
import org.apache.spark.SparkException

import scala.collection.mutable.ArrayBuffer

/**
 * AngelPSClient is a Angel PS implement of PSClient. AngelPSClient builds a bridge between
 * Spark-on-Angel and Angel, AngelPSClient implements all `PSClient` `do*`s operations by calling
 * Angel's operations.
 */
private[spark] class AngelPSClient(psContext: PSContext) extends PSClient {

  /**
   * The mergeCache is used for increment/mergeMax/mergeMin of [[RemotePSVector]].
   * Increment/mergeMax/mergeMin is called on the Spark Executors. In order to reduce the times of
   * updating PS nodes, executors merge the update Vectors in local and flush the merged result to
   * PS nodes after processing all data in one executor.
   */
  private val mergeCache = new ConcurrentHashMap[Int, ArrayBuffer[RemotePSVector]]()

  private[spark] override def register(vector: PSVector): Unit = {
    require(vector.isInstanceOf[RemotePSVector], "vector can only be RemotePSVector")
    val taskId = PSContext.getTaskId()
    if (!mergeCache.contains(taskId)) {
      mergeCache.put(taskId, ArrayBuffer.empty)
    }
    mergeCache.get(taskId) += vector.asInstanceOf[RemotePSVector]
  }

  protected def doPush(to: PSModelProxy, value: Array[Double]): Unit = {
    update(to.poolId, new Push(to.poolId, to.id, value))
  }

  protected def doFill(to: PSModelProxy, value: Double): Unit = {
    update(to.poolId, new Fill(to.poolId, to.id, value))
  }

  protected def doPull(vector: PSModelProxy): Array[Double] = {
    aggregate(vector.poolId, new Pull(vector.poolId, vector.id))
      .asInstanceOf[GetRowResult].getRow.asInstanceOf[DenseDoubleVector].getValues
  }

  protected def doRandomUniform(
      to: PSModelProxy,
      min: Double,
      max: Double): Unit = {
    update(to.poolId, new RandomUniform(to.poolId, to.id, min, max))
  }

  protected def doRandomNormal(
      to: PSModelProxy,
      mean: Double,
      stddev: Double): Unit = {
    update(to.poolId, new RandomNormal(to.poolId, to.id, mean, stddev))
  }

  protected def doEqual(vector1: PSModelProxy, vector2: PSModelProxy): Boolean = {
    val res = aggregate(vector1.poolId, new Equal(vector1.poolId, vector1.id, vector2.id))
      .asInstanceOf[ScalarAggrResult].getResult
    res == 1.0
  }

  protected def doSum(vector: PSModelProxy): Double = {
    aggregate(vector.poolId, new Sum(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doMax(vector: PSModelProxy): Double = {
    aggregate(vector.poolId, new Max(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doMin(vector: PSModelProxy): Double = {
    aggregate(vector.poolId, new Min(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doNnz(vector: PSModelProxy): Int = {
    aggregate(vector.poolId, new Nnz(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult.toInt
  }

  protected def doAdd(from: PSModelProxy, value: Double, to: PSModelProxy): Unit = {
    update(from.poolId, new AddS(from.poolId, from.id, to.id, value))
  }

  protected def doMul(from: PSModelProxy, value: Double, to: PSModelProxy): Unit = {
    update(from.poolId, new MulS(from.poolId, from.id, to.id, value))
  }

  protected def doDiv(from: PSModelProxy, value: Double, to: PSModelProxy): Unit = {
    update(from.poolId, new DivS(from.poolId, from.id, to.id, value))
  }

  protected def doPow(from: PSModelProxy, value: Double, to: PSModelProxy): Unit = {
    update(from.poolId, new Pow(from.poolId, from.id, to.id, value))
  }

  protected def doSqrt(from: PSModelProxy, to: PSModelProxy): Unit = {
    update(from.poolId, new Sqrt(from.poolId, from.id, to.id))
  }

  protected def doExp(from: PSModelProxy, to: PSModelProxy): Unit = {
    update(from.poolId, new Exp(from.poolId, from.id, to.id))
  }

  protected def doExpm1(from: PSModelProxy, to: PSModelProxy): Unit = {
    update(from.poolId, new Expm1(from.poolId, from.id, to.id))
  }

  protected def doLog(from: PSModelProxy, to: PSModelProxy): Unit = {
    update(from.poolId, new Log(from.poolId, from.id, to.id))
  }

  protected def doLog1p(from: PSModelProxy, to: PSModelProxy): Unit = {
    update(from.poolId, new Log1p(from.poolId, from.id, to.id))
  }

  protected def doLog10(from: PSModelProxy, to: PSModelProxy): Unit = {
    update(from.poolId, new Log10(from.poolId, from.id, to.id))
  }

  protected def doCeil(from: PSModelProxy, to: PSModelProxy): Unit = {
    update(from.poolId, new Ceil(from.poolId, from.id, to.id))
  }

  protected def doFloor(from: PSModelProxy, to: PSModelProxy): Unit = {
    update(from.poolId, new Floor(from.poolId, from.id, to.id))
  }

  protected def doRound(from: PSModelProxy, to: PSModelProxy): Unit = {
    update(from.poolId, new Round(from.poolId, from.id, to.id))
  }

  protected def doAbs(from: PSModelProxy, to: PSModelProxy): Unit = {
    update(from.poolId, new Abs(from.poolId, from.id, to.id))
  }

  protected def doSignum(from: PSModelProxy, to: PSModelProxy): Unit = {
    update(from.poolId, new Signum(from.poolId, from.id, to.id))
  }

  protected def doAdd(
      from1: PSModelProxy,
      from2: PSModelProxy,
      to: PSModelProxy): Unit = {
    update(from1.poolId, new Add(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doSub(
      from1: PSModelProxy,
      from2: PSModelProxy,
      to: PSModelProxy): Unit = {
    update(from1.poolId, new Sub(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doMul(
      from1: PSModelProxy,
      from2: PSModelProxy,
      to: PSModelProxy): Unit = {
    update(from1.poolId, new Mul(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doDiv(
      from1: PSModelProxy,
      from2: PSModelProxy,
      to: PSModelProxy): Unit = {
    update(from1.poolId, new Div(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doMax(
      from1: PSModelProxy,
      from2: PSModelProxy,
      to: PSModelProxy): Unit = {
    update(from1.poolId, new MaxV(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doMin(
      from1: PSModelProxy,
      from2: PSModelProxy,
      to: PSModelProxy): Unit = {
    update(from1.poolId, new MinV(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doMap(from: PSModelProxy,
      func: MapFunc,
      to: PSModelProxy): Unit = {
    update(from.poolId, new Map(from.poolId, from.id, to.id, func))
  }

  protected def doZip2Map(
      from1: PSModelProxy,
      from2: PSModelProxy,
      func: Zip2MapFunc,
      to: PSModelProxy): Unit = {
    update(from1.poolId, new Zip2Map(from1.poolId, from1.id, from2.id, to.id, func))
  }

  protected def doZip3Map(
      from1: PSModelProxy,
      from2: PSModelProxy,
      from3: PSModelProxy,
      func: Zip3MapFunc,
      to: PSModelProxy): Unit = {
    update(from1.poolId,
      new Zip3Map(from1.poolId, from1.id, from2.id, from3.id, to.id, func))
  }

  protected def doMapWithIndex(
      from: PSModelProxy,
      func: MapWithIndexFunc,
      to: PSModelProxy): Unit = {
    update(from.poolId, new MapWithIndex(from.poolId, from.id, to.id, func))
  }

  protected def doZip2MapWithIndex(
      from1: PSModelProxy,
      from2: PSModelProxy,
      func: Zip2MapWithIndexFunc,
      to: PSModelProxy): Unit = {
    update(from1.poolId,
      new Zip2MapWithIndex(from1.poolId, from1.id, from2.id, to.id, func))
  }

  protected def doZip3MapWithIndex(
      from1: PSModelProxy,
      from2: PSModelProxy,
      from3: PSModelProxy,
      func: Zip3MapWithIndexFunc,
      to: PSModelProxy): Unit = {
    update(from1.poolId,
      new Zip3MapWithIndex(from1.poolId, from1.id, from2.id, from3.id, to.id, func))
  }

  protected def doAxpy(a: Double, x: PSModelProxy, y: PSModelProxy): Unit = {
    update(x.poolId, new Axpy(x.poolId, x.id, y.id, a))
  }

  protected def doDot(x: PSModelProxy, y: PSModelProxy): Double = {
    aggregate(x.poolId, new Dot(x.poolId, x.id, y.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doCopy(x: PSModelProxy, y: PSModelProxy): Unit = {
    update(x.poolId, new Copy(x.poolId, x.id, y.id))
  }

  protected def doScal(a: Double, x: PSModelProxy): Unit = {
    update(x.poolId, new Scale(x.poolId, x.id, a))
  }

  protected def doNrm2(x: PSModelProxy): Double = {
    aggregate(x.poolId, new Nrm2(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doAsum(x: PSModelProxy): Double = {
    aggregate(x.poolId, new Asum(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doAmax(x: PSModelProxy): Double = {
    aggregate(x.poolId, new Amax(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doAmin(x: PSModelProxy): Double = {
    aggregate(x.poolId, new Amin(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doIncrement(vector: PSModelProxy, delta: Array[Double]): Unit = {
    update(vector.poolId, new Increment(vector.poolId, vector.id, delta))
  }

  protected def doMergeMax(vector: PSModelProxy, other: Array[Double]): Unit = {
    update(vector.poolId, new MaxA(vector.poolId, vector.id, other))
  }

  protected def doMergeMin(vector: PSModelProxy, other: Array[Double]): Unit = {
    update(vector.poolId, new MinA(vector.poolId, vector.id, other))
  }

  override protected def doFlush(): Unit = {
    val taskId = PSContext.getTaskId()
    if (mergeCache.containsKey(taskId)) {
      val buffer = mergeCache.get(taskId)
      buffer.foreach(_.flush())
      buffer.clear()
    }
  }

  /**
   * Use PS Oriented Function(POF) to update a PSVector on PS nodes.
   *
   * @param poolId The PSVectorPool id(matrix id in Angel)
   * @param func update PS Oriented Function(POF)
   */
  private def update(poolId: Int, func: UpdateFunc): Unit = {
    val client = MatrixClientFactory.get(poolId, PSContext.getTaskId())
    val result = client.update(func).get
    assertSuccess(result)
  }

  /**
   * Use PS Oriented Function(POF) to aggregate a PSVector on PS nodes.
   *
   * @param poolId PSVectorPool id(matrix id in Angel)
   * @param func aggregate PS Oriented Function(POF)
   * @return the result of aggregate function
   */
  private def aggregate(poolId: Int, func: GetFunc): GetResult = {
    val client = MatrixClientFactory.get(poolId, PSContext.getTaskId())
    val result = client.get(func)
    assertSuccess(result)
    result
  }

  private def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new SparkException("PS computation failed!")
    }
  }
}
