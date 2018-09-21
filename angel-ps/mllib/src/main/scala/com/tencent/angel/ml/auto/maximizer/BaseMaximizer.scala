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


package com.tencent.angel.ml.auto.maximizer

import com.tencent.angel.ml.auto.acquisition.BaseAcquisition
import com.tencent.angel.ml.math2.vector.Vector

/**
  * Abstract base class for acquisition maximization.
  * @param acquisition : The acquisition function which will be maximized
  */
abstract class BaseMaximizer(acqFunc: BaseAcquisition) {

  /**
    * Maximizes the given acquisition function.
    *
    * @param batchSize : Number of queried points.
    * @return A set of points with highest acquisition value.
    */
  def maximize(batchSize: Int): Array[Vector]

  def maximize: Vector
}
