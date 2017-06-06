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

package com.tencent.angel.spark.models

import org.apache.spark.SparkException

import com.tencent.angel.spark.PSContext
import com.tencent.angel.spark.models.vector.{BreezePSVector, RemotePSVector}

/**
 * PSVectorProxy is a proxy of PSVector stored on the PS nodes.
 * We recommend to use PSVectorProxy type as parameter when programming, and call `mkLocal`,
 * `mkRemote` and `mkBreeze` to obtain LocalPSVector, RemotePSVector and BreezePSVector.
 *
 * LocalPSVector, RemotePSVector and BreezePSVector have implement a set of operations for
 * difference situation.
 */
class PSModelProxy private[spark](
    @transient private var _pool: PSModelPool,
    private[spark] val poolId: Int,
    private[spark] val id: Int,
    val numDimensions: Int) extends Serializable {

  @transient private var deleted = false

  private def pool = {
    if (_pool == null) {
      _pool = PSContext.getOrCreate().getPool(poolId)
    }
    _pool
  }

  /**
   * Return PSVectorPool which this PSVector belongs to.
   * It only can be called on driver.
   */
  def getPool(): PSModelPool = {
    assertValid()
    pool
  }

  /**
   * Generate a RemotePSVector for this PSVectorKey
   */
  def mkRemote(): RemotePSVector = {
    assertValid()
    new RemotePSVector(this)
  }

  /**
   * Generate a BreezePSVector for this PSVectorKey
   */
  def mkBreeze(): BreezePSVector = {
    assertValid()
    new BreezePSVector(this)
  }

  /**
   * Delete this PSVector, PSVectorPool will recycle its space in pool
   */
  def delete(): Unit = {
    if (!deleted) {
      pool.delete(this)
      deleted = true
    }
  }

  private[spark] def assertCompatible(other: PSModelProxy): Unit = {
    if (this.poolId != other.poolId) {
      throw new SparkException("BLAS operators can only " +
        "be performed on vectors of the same pool!")
    }
  }

  private[spark] def assertCompatible(other: Array[Double]): Unit = {
    if (this.numDimensions != other.length) {
      throw new SparkException(s"The target array's dimension " +
        s"does not match this vector pool! \n" +
        s"pool dimension is $numDimensions," +
        s"but target array's dimension is ${other.length}")
    }
  }

  private[spark] def assertValid(): Unit = {
    if (deleted) {
      throw new SparkException("This vector has been deleted!")
    }
  }

  override def equals(other: Any): Boolean = {
    other match {
      case k: PSModelProxy =>
        this.poolId == k.poolId && this.id == k.id
      case _ => false
    }
  }

  override def hashCode(): Int = {
    poolId * 31 + id
  }

}
