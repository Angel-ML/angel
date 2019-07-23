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


package com.tencent.angel.spark.automl.tuner.acquisition

import org.apache.spark.ml.linalg.Vector
import com.tencent.angel.spark.automl.tuner.surrogate.Surrogate


/**
  * Abstract base class for acquisition function
  */
abstract class Acquisition(val surrogate: Surrogate) {

  /**
    * Computes the acquisition value for a given point X
    *
    * @param X : (1, D), the input points where the acquisition function should be evaluated.
    * @return (1, 1) Expected Improvement of X, (1, D) Derivative of Expected Improvement at X
    */
  def compute(X: Vector, derivative: Boolean = false): (Double, Vector)

}
