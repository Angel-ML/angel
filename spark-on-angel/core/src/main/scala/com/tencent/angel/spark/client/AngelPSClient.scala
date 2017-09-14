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

import com.tencent.angel.spark.math.vector.PSVector
import org.apache.spark.SparkException

import com.tencent.angel.ml.math.vector.DenseDoubleVector
import com.tencent.angel.ml.matrix.psf.aggr._
import com.tencent.angel.ml.matrix.psf.aggr.enhance.{FullAggrResult, ScalarAggrResult}
import com.tencent.angel.ml.matrix.psf.aggr.primitive.Pull
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult
import com.tencent.angel.ml.matrix.psf.update._
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc
import com.tencent.angel.ml.matrix.psf.update.enhance.map.{Map, MapFunc, MapInPlace, MapWithIndex, MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.{Zip2Map, Zip2MapFunc, Zip2MapWithIndex, Zip2MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.enhance.zip3.{Zip3Map, Zip3MapFunc, Zip3MapWithIndex, Zip3MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.primitive.{Increment, Push}
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.math.matrix.PSMatrix

/**
 * AngelPSClient is a Angel PS implement of PSClient. AngelPSClient builds a bridge between
 * Spark-on-Angel and Angel, AngelPSClient implements all `PSClient` `do*`s operations by calling
 * Angel's operations.
 */
private[spark] class AngelPSClient(psContext: PSContext) extends PSClient {

  /**
    * ===================================================
    * The following methods are vector oriented.
    * ===================================================
    */
  /**
    * Use ps Function(PSF) to update a PSVector on PS nodes.
    */
  private def update(modelId: Int, func: UpdateFunc): Unit = {
    val client = MatrixClientFactory.get(modelId, PSContext.getTaskId())
    val result = client.update(func).get
    assertSuccess(result)
  }

  protected def doPush(to: PSVector, value: Array[Double]): Unit = {
    update(to.poolId, new Push(to.poolId, to.id, value))
  }

  protected def doFill(to: PSVector, value: Double): Unit = {
    update(to.poolId, new Fill(to.poolId, to.id, value))
  }


  protected def doRandomUniform(to: PSVector, min: Double, max: Double) = {
    update(to.poolId, new RandomUniform(to.poolId, to.id, min, max))
  }

  protected def doRandomNormal(to: PSVector, mean: Double, stddev: Double): Unit = {
    update(to.poolId, new RandomNormal(to.poolId, to.id, mean, stddev))
  }


  /**
   * Use ps Function(PSF) to aggregate a PSVector on PS nodes.
   */
  private def aggregate(modelId: Int, func: GetFunc): GetResult = {
    val client = MatrixClientFactory.get(modelId, PSContext.getTaskId())
    val result = client.get(func)
    assertSuccess(result)
    result
  }

  protected def doPull(vector: PSVector): Array[Double] = {
    aggregate(vector.poolId, new Pull(vector.poolId, vector.id))
      .asInstanceOf[GetRowResult]
      .getRow.asInstanceOf[DenseDoubleVector].getValues
  }

  protected def doEqual(vector1: PSVector, vector2: PSVector): Boolean = {
    val res = aggregate(vector1.poolId, new Equal(vector1.poolId, vector1.id, vector2.id))
      .asInstanceOf[ScalarAggrResult].getResult
    res == 1.0
  }



  /**
   * ===================================================
   * The following methods are matrix oriented.
   * ===================================================
   */

  protected def doPull(mat: PSMatrix): Array[Array[Double]] = {
    aggregate(mat, new FullPull(mat.meta.getId))
      .asInstanceOf[FullAggrResult].getResult
  }

  protected def doRandom(mat: PSMatrix): Unit = {
    update(mat, new Random(mat.meta.getId))
  }

  protected def doDiag(mat: PSMatrix, values: Array[Double]): Unit = {
    update(mat, new Diag(mat.meta.getId, values))
  }

  protected def doEye(mat: PSMatrix): Unit = {
    update(mat, new Eye(mat.meta.getId))
  }

  protected def doFill(mat: PSMatrix, value: Double): Unit = {
    update(mat, new FullFill(mat.meta.getId, value))
  }

  private def update(matrix: PSMatrix, func: UpdateFunc): Unit = {
    val client = MatrixClientFactory.get(matrix.meta.getId, PSContext.getTaskId())
    val result = client.update(func).get()
    assertSuccess(result)
  }

  private def aggregate(matrix: PSMatrix, func: GetFunc): GetResult = {
    val client = MatrixClientFactory.get(matrix.meta.getId, PSContext.getTaskId())
    val result = client.get(func)
    assertSuccess(result)
    result
  }


  private def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new SparkException("PS computation failed!")
    }
  }



  protected def doSum(vector: PSVector): Double = {
    aggregate(vector.poolId, new Sum(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doMax(vector: PSVector): Double = {
    aggregate(vector.poolId, new Max(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doMin(vector: PSVector): Double = {
    aggregate(vector.poolId, new Min(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doNnz(vector: PSVector): Int = {
    aggregate(vector.poolId, new Nnz(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult.toInt
  }

  protected def doAdd(from: PSVector, value: Double, to: PSVector): Unit = {
    update(from.poolId, new AddS(from.poolId, from.id, to.id, value))
  }

  protected def doMul(from: PSVector, value: Double, to: PSVector): Unit = {
    update(from.poolId, new MulS(from.poolId, from.id, to.id, value))
  }

  protected def doDiv(from: PSVector, value: Double, to: PSVector): Unit = {
    update(from.poolId, new DivS(from.poolId, from.id, to.id, value))
  }

  protected def doPow(from: PSVector, value: Double, to: PSVector): Unit = {
    update(from.poolId, new Pow(from.poolId, from.id, to.id, value))
  }

  protected def doSqrt(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Sqrt(from.poolId, from.id, to.id))
  }

  protected def doExp(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Exp(from.poolId, from.id, to.id))
  }

  protected def doExpm1(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Expm1(from.poolId, from.id, to.id))
  }

  protected def doLog(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Log(from.poolId, from.id, to.id))
  }

  protected def doLog1p(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Log1p(from.poolId, from.id, to.id))
  }

  protected def doLog10(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Log10(from.poolId, from.id, to.id))
  }

  protected def doCeil(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Ceil(from.poolId, from.id, to.id))
  }

  protected def doFloor(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Floor(from.poolId, from.id, to.id))
  }

  protected def doRound(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Round(from.poolId, from.id, to.id))
  }

  protected def doAbs(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Abs(from.poolId, from.id, to.id))
  }

  protected def doSignum(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Signum(from.poolId, from.id, to.id))
  }

  protected def doAdd(
                       from1: PSVector,
                       from2: PSVector,
                       to: PSVector): Unit = {
    update(from1.poolId, new Add(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doSub(
                       from1: PSVector,
                       from2: PSVector,
                       to: PSVector): Unit = {
    update(from1.poolId, new Sub(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doMul(
                       from1: PSVector,
                       from2: PSVector,
                       to: PSVector): Unit = {
    update(from1.poolId, new Mul(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doDiv(
                       from1: PSVector,
                       from2: PSVector,
                       to: PSVector): Unit = {
    update(from1.poolId, new Div(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doMax(
                       from1: PSVector,
                       from2: PSVector,
                       to: PSVector): Unit = {
    update(from1.poolId, new MaxV(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doMin(
                       from1: PSVector,
                       from2: PSVector,
                       to: PSVector): Unit = {
    update(from1.poolId, new MinV(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doMap(from: PSVector,
                      func: MapFunc,
                      to: PSVector): Unit = {
    update(from.poolId, new Map(from.poolId, from.id, to.id, func))
  }

  protected def doMapInPlace(proxy: PSVector, func: MapFunc): Unit = {
    update(proxy.poolId, new MapInPlace(proxy.poolId, proxy.id, func))
  }

  protected def doZip2Map(
                           from1: PSVector,
                           from2: PSVector,
                           func: Zip2MapFunc,
                           to: PSVector): Unit = {
    update(from1.poolId, new Zip2Map(from1.poolId, from1.id, from2.id, to.id, func))
  }

  protected def doZip3Map(
                           from1: PSVector,
                           from2: PSVector,
                           from3: PSVector,
                           func: Zip3MapFunc,
                           to: PSVector): Unit = {
    update(from1.poolId,
      new Zip3Map(from1.poolId, from1.id, from2.id, from3.id, to.id, func))
  }

  protected def doMapWithIndex(
                                from: PSVector,
                                func: MapWithIndexFunc,
                                to: PSVector): Unit = {
    update(from.poolId, new MapWithIndex(from.poolId, from.id, to.id, func))
  }

  protected def doZip2MapWithIndex(
                                    from1: PSVector,
                                    from2: PSVector,
                                    func: Zip2MapWithIndexFunc,
                                    to: PSVector): Unit = {
    update(from1.poolId,
      new Zip2MapWithIndex(from1.poolId, from1.id, from2.id, to.id, func))
  }

  protected def doZip3MapWithIndex(
                                    from1: PSVector,
                                    from2: PSVector,
                                    from3: PSVector,
                                    func: Zip3MapWithIndexFunc,
                                    to: PSVector): Unit = {
    update(from1.poolId,
      new Zip3MapWithIndex(from1.poolId, from1.id, from2.id, from3.id, to.id, func))
  }

  protected def doAxpy(a: Double, x: PSVector, y: PSVector): Unit = {
    update(x.poolId, new Axpy(x.poolId, x.id, y.id, a))
  }

  protected def doDot(x: PSVector, y: PSVector): Double = {
    aggregate(x.poolId, new Dot(x.poolId, x.id, y.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doCopy(x: PSVector, y: PSVector): Unit = {
    update(x.poolId, new Copy(x.poolId, x.id, y.id))
  }

  protected def doScal(a: Double, x: PSVector): Unit = {
    update(x.poolId, new Scale(x.poolId, x.id, a))
  }

  protected def doNrm2(x: PSVector): Double = {
    aggregate(x.poolId, new Nrm2(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doAsum(x: PSVector): Double = {
    aggregate(x.poolId, new Asum(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doAmax(x: PSVector): Double = {
    aggregate(x.poolId, new Amax(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doAmin(x: PSVector): Double = {
    aggregate(x.poolId, new Amin(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doIncrement(vector: PSVector, delta: Array[Double]): Unit = {
    update(vector.poolId, new Increment(vector.poolId, vector.id, delta))
  }

  protected def doMergeMax(vector: PSVector, other: Array[Double]): Unit = {
    update(vector.poolId, new MaxA(vector.poolId, vector.id, other))
  }

  protected def doMergeMin(vector: PSVector, other: Array[Double]): Unit = {
    update(vector.poolId, new MinA(vector.poolId, vector.id, other))
  }
}
