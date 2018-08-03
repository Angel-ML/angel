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

package com.tencent.angel.spark.models.vector.cache

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.linalg.{DenseVector, SparseVector, Vector}
import com.tencent.angel.spark.models.vector.{DensePSVector, PSVector, SparsePSVector, VectorType}


object Local2RemoteOps {
  def pull(vector: PSVector): Vector = {
    vector.getComponent match {
      case dv: DensePSVector => PSClient.instance().denseRowOps.pull(dv)
      case sv: SparsePSVector => PSClient.instance().sparseRowOps.pull(sv)
    }
  }

  def push(vector: PSVector, local: Vector) = {
    vector.getComponent match {
      case dv: DensePSVector =>
        local match {
          case ldv: DenseVector => PSClient.instance().denseRowOps.push(dv, ldv)
          case lsv: SparseVector =>
            throw new RuntimeException("can not push a local sparse vector to dense ps vector")
        }
      case sv: SparsePSVector =>
        local match {
          case ldv: DenseVector =>
            throw new RuntimeException("can not push a local dense vector to sparse ps vector")
          case lsv: SparseVector => PSClient.instance().sparseRowOps.push(sv, lsv)
        }
    }
  }


  def increment(vector: PSVector, local: Vector) = {
    vector.getComponent match {
      case dv: DensePSVector =>
        local match {
          case ldv: DenseVector => PSClient.instance().denseRowOps.increment(dv, ldv)
          case lsv: SparseVector =>
            throw new RuntimeException("can not increment a local sparse vector to dense ps vector")
        }
      case sv: SparsePSVector =>
        local match {
          case ldv: DenseVector =>
            throw new RuntimeException("can not increment a local dense vector to sparse ps vector")
          case lsv: SparseVector => PSClient.instance().sparseRowOps.increment(sv, lsv)
        }
    }
  }

  import com.tencent.angel.spark.models.vector.VectorType.VectorType
  def increment(poolId: Int, vectorId: Int, vType: VectorType, local: Vector) = {
    vType match {
      case VectorType.DENSE =>
        local match {
          case ldv: DenseVector =>
            PSClient.instance().denseRowOps.increment(poolId, vectorId, ldv.values)
          case lsv: SparseVector =>
            throw new RuntimeException("can not increment a local sparse vector to dense ps vector")
        }
      case VectorType.SPARSE =>
        local match {
          case ldv: DenseVector =>
            throw new RuntimeException("can not increment a local dense vector to sparse ps vector")
          case lsv: SparseVector =>
            PSClient.instance().sparseRowOps.increment(poolId, vectorId, lsv)
        }
    }
  }


  def mergeMax(vector: PSVector, local: Vector) = {
    vector.getComponent match {
      case dv: DensePSVector =>
        local match {
          case ldv: DenseVector => PSClient.instance().denseRowOps.mergeMax(dv, ldv)
          case lsv: SparseVector =>
            throw new RuntimeException("can not mergeMax a local sparse vector to dense ps vector")
        }
      case sv: SparsePSVector => throw new RuntimeException("can not mergeMin to sparse ps vector")
    }
  }

  def mergeMax(poolId: Int, vectorId: Int, vType: VectorType, local: Vector) = {
    local match {
      case dv: DenseVector =>
        if (vType == VectorType.DENSE) {
          PSClient.instance().denseRowOps.mergeMax(poolId, vectorId, dv.values)
        } else {
          throw new RuntimeException("can not mergeMax a local sparse vector to dense ps vector")
        }
      case _ => throw new RuntimeException("can not mergeMin to sparse ps vector")
    }
  }


  def mergeMin(vector: PSVector, local: Vector) = {
    vector.getComponent match {
      case dv: DensePSVector =>
        local match {
          case ldv: DenseVector => PSClient.instance().denseRowOps.mergeMin(dv, ldv)
          case lsv: SparseVector =>
            throw new RuntimeException("can not mergeMin a local sparse vector to dense ps vector")
        }
      case sv: SparsePSVector => throw new RuntimeException("can not mergeMin to sparse ps vector")
    }
  }

  def mergeMin(poolId: Int, vectorId: Int, vType: VectorType, local: Vector) =   {
    local match {
      case dv: DenseVector =>
        if (vType == VectorType.DENSE) {
          PSClient.instance().denseRowOps.mergeMin(poolId, vectorId, dv.values)
        } else {
          throw new RuntimeException("can not mergeMin a local sparse vector to dense ps vector")
        }
      case _ => throw new RuntimeException("can not mergeMin to sparse ps vector")
    }
  }


}
