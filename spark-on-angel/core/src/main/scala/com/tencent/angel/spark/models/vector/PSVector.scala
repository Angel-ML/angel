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


package com.tencent.angel.spark.models.vector

import java.util.concurrent.Future

import org.apache.spark.SparkException

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update._
import com.tencent.angel.ml.matrix.psf.update.base.{UpdateFunc, VoidResult}
import com.tencent.angel.psagent.matrix.{MatrixClient, MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.models.PSModel
import com.tencent.angel.spark.models.vector.enhanced._
import com.tencent.angel.ml.matrix.psf.update.enhance.map.MapInPlace
import com.tencent.angel.ml.matrix.psf.update.enhance.map.func.{Set => SetFunc}

/**
  * PSVector is a vector store on the PS nodes, and PSVectorProxy is the proxy of PSVector.
  * PSVector has three forms: LocalPSVector, RemotePSVector and BreezePSVector,
  * these three forms of PSVector have implement a set of operations for different situation.
  * LocalPSVector implements the operations for PSVector local form.
  * RemotePSVector implements the operations between PSVector and local data.
  * BreezePSVector implements the operations among PSVectors on PS nodes.
  */

abstract class PSVector extends PSModel {
  val poolId: Int
  val id: Int
  val dimension: Long
  val rowType: RowType
  @transient private var deleted = false

  def pull(): Vector = vectorPoolClient.getRow(id, true)

  def pull(indices: Array[Long]): Vector = vectorPoolClient.get(id, indices)

  def pull(indices: Array[Int]): Vector = vectorPoolClient.get(id, indices)

  def increment(delta: Vector): this.type = {
    vectorPoolClient.increment(id, delta, true)
    this
  }

  def update(local: Vector): this.type = {
    vectorPoolClient.update(id, local)
    this
  }

  def push(local: Vector): this.type =
    assertValid().reset.update(local)


  def reset: this.type = {
    psfUpdate(new Reset(poolId, id)).get()
    this
  }

  private def vectorPoolClient: MatrixClient = {
    assertValid()
    PSContext.instance()
    MatrixClientFactory.get(poolId, PSContext.getTaskId())
  }

  private[spark] def assertValid(): this.type = {
    if (deleted)
      throw new SparkException("This vector has been deleted!")
    this
  }

  def psfUpdate(func: UpdateFunc): Future[VoidResult] = {
    assertValid()
    vectorPoolClient.update(func)
  }

  def psfGet(func: GetFunc): GetResult = {
    assertValid()
    val result = vectorPoolClient.get(func)
    assertSuccess(result)
    result
  }

  /**
    * Generate a BreezePSVector for this PSVectorKey
    */
  def toBreeze: BreezePSVector = {
    assertValid()
    new BreezePSVector(this.getComponent)
  }

  /**
    * Convert to DensePSVector
    */
  def toDense: DensePSVector = {
    val component = getComponent
    component match {
      case dv: DensePSVector => dv
      case _ => throw new RuntimeException("can not convert SparsePSVector to DensePSVector")
    }
  }

  /**
    * Generate a CachedPSVector for this PSVectorKey
    */
  def toCache: CachedPSVector = {
    assertValid()
    new CachedPSVector(this.getComponent)
  }

  private[spark] def getComponent: ConcretePSVector = {
    this match {
      case decorator: PSVectorDecorator => decorator.component
      case concrete: ConcretePSVector => concrete
    }
  }

  /**
    * Convert to SparsePSVector
    */
  def toSparse: SparsePSVector = {
    getComponent match {
      case sv: SparsePSVector => sv
      case _ => throw new RuntimeException("can not convert DensePSVector to SparsePSVector")
    }
  }

  def delete(): Unit = {
    PSContext.instance() match {
      case angel: AngelPSContext => angel.getPool(poolId).delete(this)
    }
    deleted = true
  }

  override def equals(other: Any): Boolean = {
    other match {
      case k: PSVector =>
        this.poolId == k.poolId && this.id == k.id
      case _ => false
    }
  }

  override def hashCode(): Int = {
    (poolId + "_" + id).hashCode
  }

  override def toString: String = {
    s"poolId: $poolId vectorId: $id"
  }

  def one(): this.type = {
    fill(1.0)
  }

  /**
    * Fill PSVectorKey with `value`
    * Notice: it can only be called in th driver.
    */
  def fill(value: Double): this.type = {
    assertValid()
    psfUpdate(new MapInPlace(poolId, id, new SetFunc(value))).get()
    this
  }

  private[spark] def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new AngelException("PS computation failed!")
    }
  }

  def zero(): this.type = {
    fill(0.0)
  }

  private[spark] def assertCompatible(others: PSVector*): Unit = {
    for (other <- others) {
      if (this.poolId != other.poolId) {
        throw new SparkException("Operators can only " +
          "be performed on vectors of the same pool!")
      }
    }
  }

  private[spark] def assertCompatible(other: Array[Double]): Unit = {
    if (this.dimension != other.length) {
      throw new SparkException(s"The target array's dimension " +
        s"does not match this vector pool! \n" +
        s"pool dimension is $dimension," +
        s"but target array's dimension is ${other.length}")
    }
  }

  def randomUniform(min: Double, max: Double): this.type = {
    assertValid()
    psfUpdate(new RandomUniform(poolId, id, min, max)).get()
    this
  }

  def randomNormal(mean: Double, stddev: Double): this.type = {
    assertValid()
    psfUpdate(new RandomNormal(poolId, id, mean, stddev)).get()
    this
  }

}

object PSVector {

  def duplicate[K <: PSVector](original: K): K = {
    PSContext.instance().duplicateVector(original).asInstanceOf[K]
  }

  def dense(dimension: Int, capacity: Int = 20, rowType: RowType = RowType.T_DOUBLE_DENSE): DensePSVector = {
    PSContext.instance().createVector(dimension, rowType, capacity, dimension)
      .asInstanceOf[DensePSVector]
  }

  /**
    * @param maxRange if maxRange > 0, colId range is [0, maxRange),
    *                 if maxRange = -1, colId range is (Long.MinValue, Long.MaxValue)
    */
  def longKeySparse(dim: Long, maxRange: Long, capacity: Int = 20, rowType: RowType = RowType.T_DOUBLE_SPARSE_LONGKEY): SparsePSVector = {
    sparse(dim, capacity, maxRange, rowType)
  }

  def sparse(dimension: Long, capacity: Int = 20, rowType: RowType = RowType.T_DOUBLE_SPARSE_LONGKEY): SparsePSVector = {
    sparse(dimension, capacity, dimension, rowType)
  }

  def sparse(dimension: Long, capacity: Int, range: Long, rowType: RowType): SparsePSVector = {
    PSContext.instance().createVector(dimension, rowType, capacity, range)
      .asInstanceOf[SparsePSVector]
  }

}
