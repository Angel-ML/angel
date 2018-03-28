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

/**
 *
 * This class is a copy of OWLQN.scala in breeze.optimize package of Breeze, whose author is dlwh.
 *
 * Based on the original version, we improve the algorithm with Angel PS-Service.
 */

package com.tencent.angel.spark.ml.optimize

import breeze.linalg._
import breeze.math._
import breeze.optimize.{BacktrackingLineSearch, DiffFunction, LBFGS}
import breeze.util._

import com.tencent.angel.spark.ml.psf._
import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector

/**
 * Implements the Orthant-wise Limited Memory QuasiNewton method,
 * which is a variant of LBFGS that handles L1 regularization.
 *
 * Paper is Andrew and Gao (2007) Scalable Training of L1-Regularized Log-Linear Models
 *
 */
class OWLQN(maxIter: Int, m: Int, l1reg: BreezePSVector, tolerance: Double)(
    implicit space: MutableInnerProductModule[BreezePSVector, Double])
  extends LBFGS[BreezePSVector](maxIter, m, tolerance = tolerance) with SerializableLogging {
  private var l1Param: Double = 0.0

  def this(maxIter: Int, m: Int, l1reg: BreezePSVector)(
      implicit space: MutableInnerProductModule[BreezePSVector, Double]) =
    this(maxIter, m, l1reg, 1E-8)

  def this(maxIter: Int, m: Int, l1Param: Double)(
      implicit space: MutableInnerProductModule[BreezePSVector, Double]) = {
    this(maxIter, m, null, 1E-8)
    this.l1Param = l1Param
  }

  require(m > 0)

  import space._

  override protected def chooseDescentDirection(
      state: State,
      fn: DiffFunction[BreezePSVector]): BreezePSVector = {
    val descentDir = super.chooseDescentDirection(state.copy(grad = state.adjustedGradient), fn)

    // The original paper requires that the descent direction be corrected to be
    // in the same directional (within the same hypercube) as the adjusted gradient for proof.
    // Although this doesn't seem to affect the outcome that much in most of cases, there are some
    // cases where the algorithm won't converge (confirmed with the author, Galen Andrew).
    val correctedDir = descentDir.zipMap(state.adjustedGradient, new CorrectDirection)

    correctedDir
  }

  override protected def determineStepSize(
      state: State,
      f: DiffFunction[BreezePSVector],
      dir: BreezePSVector): Double = {
    val iter = state.iter

    val normGradInDir = {
      val possibleNorm = dir dot state.grad
      //      if (possibleNorm > 0) { // hill climbing is not what we want. Bad LBFGS.
      //        logger.warn("Direction of positive gradient chosen!")
      //        logger.warn("Direction is:" + possibleNorm)
      //        Reverse the direction, clearly it's a bad idea to go up
      //        dir *= -1.0
      //        dir dot state.grad
      //      } else {
      possibleNorm
      //      }
    }

    val ff = new DiffFunction[Double] {
      def calculate(alpha: Double): (Double, Double) = {
        val newX = takeStep(state, dir, alpha)
        val (v, newG) = f.calculate(newX)
        val (adjv, adjgrad) = adjust(newX, newG, v)
        // TODO not sure if this is quite right...

        // Technically speaking, this is not quite right.
        // dir should be (newX - state.x) according to the paper and the author.
        // However, in practice, this seems fine.
        // And interestingly the MSR reference implementation does the same thing (but they don't
        // do wolfe condition checks.).
        adjv -> (adjgrad dot dir)
      }
    }
    val search = new BacktrackingLineSearch(state.value, shrinkStep = if (iter < 1) 0.1 else 0.5)
    val alpha = search.minimize(ff, if (iter < 1).5 / norm(state.grad) else 1.0)

    alpha
  }

  // projects x to be on the same orthant as y
  // this basically requires that x'_i = x_i if sign(x_i) == sign(y_i), and 0 otherwise.

  override protected def takeStep(
      state: State,
      dir: BreezePSVector,
      stepSize: Double): BreezePSVector = {
    val stepped = state.x + dir * stepSize
    val orthant = computeOrthant(state.x, state.adjustedGradient)
    stepped.zipMap(orthant, new Project)
  }

  // Adds in the regularization stuff to the gradient
  override protected def adjust(
      newX: BreezePSVector,
      newGrad: BreezePSVector,
      newVal: Double): (Double, BreezePSVector) = {
    if (l1reg != null) {
      val adjValue = newVal + newX.zipMap(l1reg, new ComputeAdjustValue).sum
      val res = newX.zipMap(newGrad, l1reg, new Adjust)
      adjValue -> res
    } else {
      val adjValue = newVal + newX.map(new ComputeAdjustValue(l1Param)).sum
      val res = newX.zipMap(newGrad, new Adjust(l1Param))
      adjValue -> res
    }
  }

  private def computeOrthant(x: BreezePSVector, grad: BreezePSVector) = {
    val orth = x.zipMap(grad, new ComputeOrthant)
    orth
  }

}
