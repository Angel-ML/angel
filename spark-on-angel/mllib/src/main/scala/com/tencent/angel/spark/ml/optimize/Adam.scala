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

package com.tencent.angel.spark.ml.optimize

import breeze.linalg._
import breeze.math._
import breeze.numerics.sqrt
import breeze.numerics.pow
import breeze.optimize.{StochasticDiffFunction, StochasticGradientDescent}
import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector


/**
  *
  * An implementation of Adam SGD.
  *
  * Adam: a Method for Stochastic Optimization. ICLR, pages 1–13, 2015.
  * Published by Diederik P. Kingma and Jimmy Lei Ba.
  */


class Adam(
    defaultStepSize: Double,
    maxIter: Int,
    l2Reg: Double,
    rho1: Double = 0.9,
    rho2: Double = 0.999,
    epsilon: Double = 1e-8,
    tolerance: Double = 1e-5,
    improveTolerance: Double = 1e-4,
    minImprovementWindow: Int = 50)(
    implicit space: MutableInnerProductModule[BreezePSVector, Double])
  extends StochasticGradientDescent[BreezePSVector](defaultStepSize, maxIter, tolerance, minImprovementWindow) {

  def this(maxIter: Int)(
          implicit space: MutableInnerProductModule[BreezePSVector, Double]) = {
    this(1d, maxIter, 0.01)
  }

  import space._

  case class History(avgGrad: BreezePSVector, avgSqGrad: BreezePSVector)

  override protected def initialHistory(
                              f: StochasticDiffFunction[BreezePSVector],
                              init: BreezePSVector): History  = {
    History(zeroLike(init), zeroLike(init))
  }

  override protected def updateHistory(
                              newX: BreezePSVector,
                              newGrad: BreezePSVector,
                              newVal: Double,
                              f: StochasticDiffFunction[BreezePSVector],
                              oldState: State): History = {
    val oldAvgGrad = oldState.history.avgGrad
    val newAvgGrad = (oldAvgGrad * rho1) + newGrad * (1 - rho1)

    val oldAvgSqGrad = oldState.history.avgSqGrad
    val newAvgSqGrad = (oldAvgSqGrad * rho2) + (newGrad :* newGrad) * (1 - rho2)

    History(newAvgGrad, newAvgSqGrad)
  }

  override protected def takeStep(
                              state: State,
                              dir: BreezePSVector,
                              stepSize: Double): BreezePSVector = {
    var newAvgGrad = (state.history.avgGrad * rho1) + state.grad * (1 - rho1)
    newAvgGrad /= (1 - pow(rho1, state.iter))

    var newAvgSqGrad = (state.history.avgSqGrad * rho2) + (state.grad :* state.grad ) * (1 - rho2)
    newAvgSqGrad /= (1 - pow(rho2, state.iter))

    val delta = (newAvgGrad :/ ( BreezePSVector.math.sqrt(newAvgSqGrad) + epsilon)) * stepSize

    state.x - delta
  }

  override def determineStepSize(
                              state: State,
                              f: StochasticDiffFunction[BreezePSVector],
                              dir: BreezePSVector) = {
    defaultStepSize
  }

  override protected def adjust(
                              newX: BreezePSVector,
                              newGrad: BreezePSVector,
                              newVal: Double)= {
    val av = newVal + newX.dot(newX) * l2Reg / 2.0
    val ag = newGrad + newX * l2Reg

    av -> ag
  }

}
