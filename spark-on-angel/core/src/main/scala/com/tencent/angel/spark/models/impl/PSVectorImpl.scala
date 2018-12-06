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


package com.tencent.angel.spark.models.impl

import java.util.concurrent.Future

import org.apache.spark.SparkException

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update._
import com.tencent.angel.ml.matrix.psf.update.base.{UpdateFunc, VoidResult}
import com.tencent.angel.ml.matrix.psf.update.enhance.map.MapInPlace
import com.tencent.angel.ml.matrix.psf.update.enhance.map.func.{Set => SetFunc}
import com.tencent.angel.psagent.matrix.{MatrixClient, MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.models.PSVector

/**
 * PSVector is a vector store on the PS nodes
 */

class PSVectorImpl(val poolId: Int, val id: Int, val dimension: Long, val rowType: RowType) extends PSVector {
  override def pull(): Vector = vectorPoolClient.getRow(id, true)

  override def pull(indices: Array[Long]): Vector = {
    require(rowType.isLongKey, s"rowType=$rowType, use `pull(indices: Array[Int])` instead")
    vectorPoolClient.get(id, indices)
  }

  override def pull(indices: Array[Int]): Vector = {
    require(rowType.isIntKey, s"rowType=$rowType, use `pull(indices: Array[Long])` instead")
    vectorPoolClient.get(id, indices)
  }

  override def increment(delta: Vector): this.type = {
    require(rowType.compatible(delta.getType), s"can't increment $rowType by ${delta.getType}")
    vectorPoolClient.increment(id, delta, true)
    this
  }

  override def update(local: Vector): this.type = {
    require(rowType.compatible(local.getType), s"can't update $rowType by ${local.getType}")
    vectorPoolClient.update(id, local)
    this
  }

  override def push(local: Vector): this.type ={
    require(rowType.compatible(local.getType), s"can't push $rowType by ${local.getType}")
    assertValid().reset.update(local)
  }

  override def reset: this.type = {
    psfUpdate(new Reset(poolId, id)).get()
    this
  }

  override def psfGet(func: GetFunc): GetResult = {
    assertValid()
    val result = vectorPoolClient.get(func)
    assertSuccess(result)
    result
  }

  override def psfUpdate(func: UpdateFunc): Future[VoidResult] = {
    assertValid()
    vectorPoolClient.update(func)
  }

  private[spark] def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new AngelException("PS computation failed!")
    }
  }

  override def delete(): Unit = {
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

  /**
   * Fill PSVectorKey with `value`
   * Notice: it can only be called in th driver.
   */
  override def fill(value: Double): this.type = {
    assertValid()
    psfUpdate(new MapInPlace(poolId, id, new SetFunc(value))).get()
    this
  }

  //
  //


  private def vectorPoolClient: MatrixClient = {
    assertValid()
    PSContext.instance()
    MatrixClientFactory.get(poolId, PSContext.getTaskId)
  }

  override def assertValid(): this.type = {
    if (deleted)
      throw new SparkException("This vector has been deleted!")
    this
  }
}