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


package com.tencent.angel.spark.util

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.spark.models.vector.PSVector

object PSVectorImplicit {

  implicit class DoublePSVector(vec: PSVector) {
    def increment(dim: Long, keys: Array[Long], values: Array[Double]): Unit = {
      vec.increment(VFactory.sparseLongKeyDoubleVector(dim, keys, values))
    }

    def increment(dim: Long, keyValues: Array[(Long, Double)]): Unit = {
      val (keys, values) = keyValues.unzip
      increment(dim, keys, values)
    }

    def push(local: Array[Double]): Unit = {
      vec.assertValid()
      vec.reset.update(VFactory.denseDoubleVector(local))
    }
  }

}
