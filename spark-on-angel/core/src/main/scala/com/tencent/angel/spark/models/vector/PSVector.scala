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

package com.tencent.angel.spark.models.vector

import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.models.vector.enhanced._
import com.tencent.angel.spark.models.PSModel
import com.tencent.angel.spark.linalg.Vector
import org.apache.spark.SparkException

import com.tencent.angel.spark.models.vector.cache.Local2RemoteOps

/**
 * PSVector is a vector store on the PS nodes, and PSVectorProxy is the proxy of PSVector.
 * PSVector has three forms: LocalPSVector, RemotePSVector and BreezePSVector,
 * these three forms of PSVector have implement a set of operations for different situation.
 * LocalPSVector implements the operations for PSVector local form.
 * RemotePSVector implements the operations between PSVector and local data.
 * BreezePSVector implements the operations among PSVectors on PS nodes.
 */

abstract class PSVector extends PSModel {

  @transient private var deleted = false

  val poolId: Int
  val id: Int
  val dimension: Long

  def pull: Vector = Local2RemoteOps.pull(this)

  /**
    * Generate a CachedPSVector for this PSVectorKey
    */
  def toCache: CachedPSVector = {
    assertValid()
    new CachedPSVector(this.getComponent)
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

  private[spark] def getComponent: ConcretePSVector = {
    this match {
      case decorator: PSVectorDecorator => decorator.component
      case concrete: ConcretePSVector => concrete
    }
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

  private[spark] def assertValid(): Unit = {
    if (deleted) {
      throw new SparkException("This vector has been deleted!")
    }
  }

}

object PSVector {

  def duplicate[K <: PSVector](original: K): K = {
    PSContext.instance().duplicateVector(original).asInstanceOf[K]
  }

  def dense(dim: Int, capacity: Int = 50): DensePSVector = {
    DensePSVector.apply(dim, capacity)
  }

  /**
   * @param maxRange if maxRange > 0, colId range is [0, maxRange),
   *                 if maxRange = -1, colId range is (Long.MinValue, Long.MaxValue)
   */
  def longKeySparse(dim: Long, maxRange: Long, capacity: Int = 20): SparsePSVector = {
    SparsePSVector.apply(dim, capacity, maxRange)
  }

  def sparse(dim: Long, capacity: Int = 20): SparsePSVector = {
    SparsePSVector.apply(dim, capacity)
  }
}


object VectorType extends Enumeration {
  type VectorType = Value
  val DENSE, SPARSE = Value
}
