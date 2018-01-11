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

package com.tencent.angel.spark.models.vector.enhanced

import breeze.linalg.operators._
import breeze.linalg.support.{CanCopy, CanCreateZerosLike}
import breeze.linalg.{NumericOps, dim, norm, scaleAdd}
import breeze.math.MutableLPVectorField

import com.tencent.angel.ml.matrix.psf.update.enhance.map.{MapFunc, MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.{Zip2MapFunc, Zip2MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.enhance.zip3.{Zip3MapFunc, Zip3MapWithIndexFunc}
import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.models.vector.{ConcretePSVector, PSVector}
import org.apache.spark.SparkException
import scala.language.implicitConversions

/**
 * BreezePSVector implements a set of operations among PSVectors. BreezePSVector tries to implement
 * all operation in `breeze.linalg.Vector`, it aims to reuse the breeze algorithm for BreezePSVector
 * type.
 *
 * BreezePSVector inherits from `breeze.linalg.NumericOps`, it implements a set of implicit
 * conversion to make `breeze.linalg.NumericOps` available to BreezePSVector.
 *
 * val a: BreezePSVector = pool.createZero().mkBreeze
 * val b: BreezePSVector = pool.createRandomUniform(0, 1).mkBreeze
 *
 * val c = a + b  // "+" is an operation in `breeze.linalg.NumericOps`
 */
class BreezePSVector(component: ConcretePSVector) extends PSVectorDecorator(component) with NumericOps[BreezePSVector] {

  override val dimension: Long = component.dimension
  override val id: Int = component.id
  override val poolId: Int = component.poolId

  override def delete(): Unit = component.delete()

  // Ensure that operators are all loaded.
  BreezePSVector.init()

  override def repr: BreezePSVector = this

  import BreezePSVector._

  // creation operators
  /**
   * Create a BreezePSVector filled with zero
   */

  def copy: BreezePSVector = canCopyBreezePSVector(this)


  /**
   * Calculate p-norm of BreezePSVector
   */
  def norm(p: Int): Double = canNorm2(this, p)

  /**
   * Find the maximum element in BreezePSVector
   */
  def max: Double = psClient.vectorOps.max(this)

  /**
   * Find the minimum element in BreezePSVector
   */
  def min: Double = psClient.vectorOps.min(this)

  /**
   * Calculate summation of each BreezePSVector element
   */
  def sum: Double = psClient.vectorOps.sum(this)


  // functional operators
  /**
   * Apply `MapFunc` to each element of BreezePSVector
   */
  def map(func: MapFunc): BreezePSVector = {
    val to = PSVector.duplicate(this.component)
    psClient.vectorOps.map(this, func, to)
    to.toBreeze
  }

  /**
   * Apply `Zip2MapFunc` to this and `other` BreezePSVector
   */
  def zipMap(other: BreezePSVector, func: Zip2MapFunc): BreezePSVector = {
    val to = PSVector.duplicate(this.component)
    psClient.vectorOps.zip2Map(this, other, func, to)
    to.toBreeze
  }

  /**
   * Apply `Zip3MapFunc` to this, `other1` and `other2` BreezePSVector
   */
  def zipMap(
      other1: BreezePSVector,
      other2: BreezePSVector,
      func: Zip3MapFunc): BreezePSVector = {
    val to = PSVector.duplicate(this.component)
    psClient.vectorOps.zip3Map(this, other1, other2, func, to)
    to.toBreeze
  }

  /**
   * Apply `MapWithIndexFunc` to each element of BreezePSVector
   */
  def mapWithIndex(func: MapWithIndexFunc): BreezePSVector = {
    val to = PSVector.duplicate(this.component)
    psClient.vectorOps.mapWithIndex(this, func, to)
    to.toBreeze
  }

  /**
   * Apply `Zip2MapWithIndexFunc` to this and `other` BreezePSVector
   */
  def zipMapWithIndex(
      other: BreezePSVector,
      func: Zip2MapWithIndexFunc): BreezePSVector = {
    val to = PSVector.duplicate(this.component)
    psClient.vectorOps.zip2MapWithIndex(this, other, func, to)
    to.toBreeze
  }

  /**
   * Apply `Zip3MapWithIndexFunc` to this, `other1` and `other2` BreezePSVector
   */
  def zipMapWithIndex(
      other1: BreezePSVector,
      other2: BreezePSVector,
      func: Zip3MapWithIndexFunc): BreezePSVector = {
    val to = PSVector.duplicate(other1.component)
    psClient.vectorOps.zip3MapWithIndex(this, other1, other2, func, to)
    to.toBreeze
  }

  // mutable functional operators
  /**
   * Apply `MapFunc` to each element of BreezePSVector,
   * and save the result in this PSBreezeVector
   */
  def mapInto(func: MapFunc): Unit = {
    psClient.vectorOps. map(this, func, this)
  }

  /**
   * Apply `Zip2MapFunc` to this and `other` BreezePSVector,
   * and save the result in this PSBreezeVector
   */
  def zipMapInto(other: BreezePSVector, func: Zip2MapFunc): Unit = {
    psClient.vectorOps.zip2Map(this, other, func, this)
  }

  /**
   * Apply `Zip3MapFunc` to this, `other1` and `other2` BreezePSVector,
   * and save the result in this PSBreezeVector
   */
  def zipMapInto(
      other1: BreezePSVector,
      other2: BreezePSVector,
      func: Zip3MapFunc): Unit = {
    psClient.vectorOps.zip3Map(this, other1, other2, func, this)
  }

  /**
   * Apply `MapWithIndexFunc` to each element of BreezePSVector,
   * and save the result in this PSBreezeVector
   */
  def mapWithIndexInto(func: MapWithIndexFunc): Unit = {
    psClient.vectorOps.mapWithIndex(this, func, this)
  }

  /**
   * Apply `Zip2MapWithIndexFunc` to this and `other` BreezePSVector,
   * and save the result in this PSBreezeVector
   */
  def zipMapWithIndexInto(other: BreezePSVector, func: Zip2MapWithIndexFunc): Unit = {
    psClient.vectorOps.zip2MapWithIndex(this, other, func, this)
  }
  /**
   * Apply `Zip3MapWithIndexFunc` to this, `other1` and `other2` BreezePSVector,
   * and save the result in this PSBreezeVector
   */
  def zipMapWithIndexInto(
      other1: BreezePSVector,
      other2: BreezePSVector,
      func: Zip3MapWithIndexFunc): Unit = {
    psClient.vectorOps.zip3MapWithIndex(this, other1, other2, func, this)
  }

  override def equals(obj: scala.Any): Boolean = {
    if (obj == null) {
      false
    } else if (!obj.isInstanceOf[BreezePSVector]) {
      false
    } else {
      val brzObj = obj.asInstanceOf[BreezePSVector]
      if (brzObj.dimension != this.dimension) {
        false
      } else {
        psClient.vectorOps.equal(this, brzObj)
      }
    }
  }

}

object BreezePSVector {

  /**
   * Operations in math for BreezePSVector is corresponding to `scala.math`
   */

  object math {
    def max(x: BreezePSVector, y: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.max(x, y, to)
      to.toBreeze
    }

    def min(x: BreezePSVector, y: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.min(x, y, to)
      to.toBreeze
    }

    def pow(x: BreezePSVector, a: Double): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.pow(x, a, to)
      to.toBreeze
    }

    def sqrt(x: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.sqrt(x, to)
      to.toBreeze
    }

    def exp(x: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.exp(x, to)
      to.toBreeze
    }

    def expm1(x: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.expm1(x, to)
      to.toBreeze
    }

    def log(x: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.log(x, to)
      to.toBreeze
    }

    def log1p(x: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.log1p(x, to)
      to.toBreeze
    }

    def log10(x: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.log10(x, to)
      to.toBreeze
    }

    def ceil(x: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.ceil(x, to)
      to.toBreeze
    }

    def floor(x: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.floor(x, to)
      to.toBreeze
    }

    def round(x: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.round(x, to)
      to.toBreeze
    }

    def abs(x: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.abs(x, to)
      to.toBreeze
    }

    def signum(x: BreezePSVector): BreezePSVector = {
      val to = PSVector.duplicate(x.component)
      psClient.vectorOps.signum(x, to)
      to.toBreeze
    }


    // in place funcs

    def maxInto(x: BreezePSVector, y: BreezePSVector): Unit = {
      psClient.vectorOps.max(x, y, x)
    }

    def minInto(x: BreezePSVector, y: BreezePSVector): Unit = {
      psClient.vectorOps.min(x, y, x)
    }

    def powInto(x: BreezePSVector, a: Double): Unit = {
      psClient.vectorOps.pow(x, a, x)
    }

    def sqrtInto(x: BreezePSVector): Unit = {
      psClient.vectorOps.sqrt(x, x)
    }

    def expInto(x: BreezePSVector): Unit = {
      psClient.vectorOps.exp(x, x)
    }

    def expm1Into(x: BreezePSVector): Unit = {
      psClient.vectorOps.expm1(x, x)
    }

    def logInto(x: BreezePSVector): Unit = {
      psClient.vectorOps.log(x, x)
    }

    def log1pInto(x: BreezePSVector): Unit = {
      psClient.vectorOps.log1p(x, x)
    }

    def log10Into(x: BreezePSVector): Unit = {
      psClient.vectorOps.log10(x, x)
    }

    def ceilInto(x: BreezePSVector): Unit = {
      psClient.vectorOps.ceil(x, x)
    }

    def floorInto(x: BreezePSVector): Unit = {
      psClient.vectorOps.floor(x, x)
    }

    def roundInto(x: BreezePSVector): Unit = {
      psClient.vectorOps.round(x, x)
    }

    def absInto(x: BreezePSVector): Unit = {
      psClient.vectorOps.abs(x, x)
    }

    def signumInto(x: BreezePSVector): Unit = {
      psClient.vectorOps.signum(x, x)
    }
  }


  /**
   * These are blas operations for BreezePSVector
   */
  object blas {
    def axpy(a: Double, x: BreezePSVector, y: BreezePSVector): Unit = {
      psClient.vectorOps.axpy(a, x, y)
    }

    def dot(x: BreezePSVector, y: BreezePSVector): Double = {
      psClient.vectorOps.dot(x, y)
    }

    def copy(x: BreezePSVector, y: BreezePSVector): Unit = {
      psClient.vectorOps.copy(x, y)
    }

    def scal(a: Double, x: BreezePSVector): Unit = {
      psClient.vectorOps.scal(a, x)
    }

    def nrm2(x: BreezePSVector): Double = {
      psClient.vectorOps.nrm2(x)
    }

    def asum(x: BreezePSVector): Double = {
      psClient.vectorOps.asum(x)
    }

    def amax(x: BreezePSVector): Double = {
      psClient.vectorOps.amax(x)
    }

    def amin(x: BreezePSVector): Double = {
      psClient.vectorOps.amin(x)
    }
  }
  // capabilities

  implicit val canCreateZerosLike: CanCreateZerosLike[BreezePSVector, BreezePSVector] =
    new CanCreateZerosLike[BreezePSVector, BreezePSVector] {
      def apply(v: BreezePSVector): BreezePSVector = {
        PSVector.duplicate(v.component).toBreeze
      }
    }

  implicit val canCopyBreezePSVector: CanCopy[BreezePSVector] = {
    new CanCopy[BreezePSVector] {
      def apply(v: BreezePSVector): BreezePSVector = {
        val r = PSVector.duplicate(v.component)
        psClient.vectorOps.copy(v, r)
        r.toBreeze
      }
    }
  }

  implicit val canSetInto: OpSet.InPlaceImpl2[BreezePSVector, BreezePSVector] = {
    new OpSet.InPlaceImpl2[BreezePSVector, BreezePSVector] {
      def apply(y: BreezePSVector, x: BreezePSVector): Unit = {
        psClient.vectorOps.copy(x, y)
      }
    }
  }

  implicit val canSetIntoS: OpSet.InPlaceImpl2[BreezePSVector, Double] = {
    new OpSet.InPlaceImpl2[BreezePSVector, Double] {
      def apply(a: BreezePSVector, b: Double): Unit = {
        psClient.vectorOps.fill(a, b)
      }
    }
  }

  implicit val canAxpy: scaleAdd.InPlaceImpl3[BreezePSVector, Double, BreezePSVector] = {
    new scaleAdd.InPlaceImpl3[BreezePSVector, Double, BreezePSVector] {
      def apply(y: BreezePSVector, a: Double, x: BreezePSVector): Unit = {
        psClient.vectorOps.axpy(a, x, y)
      }
    }
  }

  implicit val canAddInto: OpAdd.InPlaceImpl2[BreezePSVector, BreezePSVector] = {
    new OpAdd.InPlaceImpl2[BreezePSVector, BreezePSVector] {
      def apply(a: BreezePSVector, b: BreezePSVector): Unit = {
        psClient.vectorOps.add(a, b, a)
      }
    }
  }

  implicit val canAdd: OpAdd.Impl2[BreezePSVector, BreezePSVector, BreezePSVector] = {
    new OpAdd.Impl2[BreezePSVector, BreezePSVector, BreezePSVector] {
      def apply(a: BreezePSVector, b: BreezePSVector): BreezePSVector = {
        val to = PSVector.duplicate(a.component)
        psClient.vectorOps.add(a, b, to)
        to.toBreeze
      }
    }
  }

  implicit val canAddIntoS: OpAdd.InPlaceImpl2[BreezePSVector, Double] = {
    new OpAdd.InPlaceImpl2[BreezePSVector, Double] {
      def apply(a: BreezePSVector, b: Double): Unit = {
        psClient.vectorOps.add(a, b, a)
      }
    }
  }

  implicit val canAddS: OpAdd.Impl2[BreezePSVector, Double, BreezePSVector] = {
    new OpAdd.Impl2[BreezePSVector, Double, BreezePSVector] {
      def apply(a: BreezePSVector, b: Double): BreezePSVector = {
        val to = PSVector.duplicate(a.component)
        psClient.vectorOps.add(a, b, to)
        to.toBreeze
      }
    }
  }

  implicit val canSubInto: OpSub.InPlaceImpl2[BreezePSVector, BreezePSVector] = {
    new OpSub.InPlaceImpl2[BreezePSVector, BreezePSVector] {
      def apply(a: BreezePSVector, b: BreezePSVector): Unit = {
        psClient.vectorOps.sub(a, b, a)
      }
    }
  }

  implicit val canSub: OpSub.Impl2[BreezePSVector, BreezePSVector, BreezePSVector] = {
    new OpSub.Impl2[BreezePSVector, BreezePSVector, BreezePSVector] {
      def apply(a: BreezePSVector, b: BreezePSVector): BreezePSVector = {
        val to = PSVector.duplicate(a.component)
        psClient.vectorOps.sub(a, b, to)
        to.toBreeze
      }
    }
  }

  implicit val canSubIntoS: OpSub.InPlaceImpl2[BreezePSVector, Double] = {
    new OpSub.InPlaceImpl2[BreezePSVector, Double] {
      def apply(a: BreezePSVector, b: Double): Unit = {
        psClient.vectorOps.sub(a, b, a)
      }
    }
  }

  implicit val canSubS: OpSub.Impl2[BreezePSVector, Double, BreezePSVector] = {
    new OpSub.Impl2[BreezePSVector, Double, BreezePSVector] {
      def apply(a: BreezePSVector, b: Double): BreezePSVector = {
        val to = PSVector.duplicate(a.component)
        psClient.vectorOps.sub(a, b, to)
        to.toBreeze
      }
    }
  }

  implicit val canMulInto: OpMulScalar.InPlaceImpl2[BreezePSVector, BreezePSVector] = {
    new OpMulScalar.InPlaceImpl2[BreezePSVector, BreezePSVector] {
      def apply(a: BreezePSVector, b: BreezePSVector): Unit = {
        psClient.vectorOps.mul(a, b, a)
      }
    }
  }

  implicit val canMul: OpMulScalar.Impl2[BreezePSVector, BreezePSVector, BreezePSVector] = {
    new OpMulScalar.Impl2[BreezePSVector, BreezePSVector, BreezePSVector] {
      def apply(a: BreezePSVector, b: BreezePSVector): BreezePSVector = {
        val to = PSVector.duplicate(a.component)
        psClient.vectorOps.mul(a, b, to)
        to.toBreeze
      }
    }
  }

  implicit val canMulIntoS: OpMulScalar.InPlaceImpl2[BreezePSVector, Double] = {
    new OpMulScalar.InPlaceImpl2[BreezePSVector, Double] {
      def apply(a: BreezePSVector, b: Double): Unit = {
        psClient.vectorOps.mul(a, b, a)
      }
    }
  }

  implicit val canMulS: OpMulScalar.Impl2[BreezePSVector, Double, BreezePSVector] = {
    new OpMulScalar.Impl2[BreezePSVector, Double, BreezePSVector] {
      def apply(a: BreezePSVector, b: Double): BreezePSVector = {
        val to = PSVector.duplicate(a.component)
        psClient.vectorOps.mul(a, b, to)
        to.toBreeze
      }
    }
  }

  implicit val negFromScale: OpNeg.Impl[BreezePSVector, BreezePSVector] = {
    val scale = implicitly[OpMulScalar.Impl2[BreezePSVector, Double, BreezePSVector]]
    new OpNeg.Impl[BreezePSVector, BreezePSVector] {
      def apply(a: BreezePSVector): BreezePSVector = {
        scale(a, -1.0)
      }
    }
  }

  implicit val canDivInto: OpDiv.InPlaceImpl2[BreezePSVector, BreezePSVector] = {
    new OpDiv.InPlaceImpl2[BreezePSVector, BreezePSVector] {
      def apply(a: BreezePSVector, b: BreezePSVector): Unit = {
        psClient.vectorOps.div(a, b, a)
      }
    }
  }

  implicit val canDiv: OpDiv.Impl2[BreezePSVector, BreezePSVector, BreezePSVector] = {
    new OpDiv.Impl2[BreezePSVector, BreezePSVector, BreezePSVector] {
      def apply(a: BreezePSVector, b: BreezePSVector): BreezePSVector = {
        val to = PSVector.duplicate(a.component)
        psClient.vectorOps.div(a, b, to)
        to.toBreeze
      }
    }
  }

  implicit val canDivIntoS: OpDiv.InPlaceImpl2[BreezePSVector, Double] = {
    new OpDiv.InPlaceImpl2[BreezePSVector, Double] {
      def apply(a: BreezePSVector, b: Double): Unit = {
        psClient.vectorOps.div(a, b, a)
      }
    }
  }

  implicit val canDivS: OpDiv.Impl2[BreezePSVector, Double, BreezePSVector] = {
    new OpDiv.Impl2[BreezePSVector, Double, BreezePSVector] {
      def apply(a: BreezePSVector, b: Double): BreezePSVector = {
        val to = PSVector.duplicate(a.component)
        psClient.vectorOps.div(a, b, to)
        to.toBreeze
      }
    }
  }

  implicit val canPow: OpPow.Impl2[BreezePSVector, Double, BreezePSVector] = {
    new OpPow.Impl2[BreezePSVector, Double, BreezePSVector] {
      def apply(a: BreezePSVector, b: Double): BreezePSVector = {
        val to = PSVector.duplicate(a.component)
        psClient.vectorOps.pow(a, b, to)
        to.toBreeze
      }
    }
  }

  implicit val canDot: OpMulInner.Impl2[BreezePSVector, BreezePSVector, Double] = {
    new OpMulInner.Impl2[BreezePSVector, BreezePSVector, Double] {
      def apply(a: BreezePSVector, b: BreezePSVector): Double = {
        psClient.vectorOps.dot(a, b)
      }
    }
  }

  /**
   * Returns the 2-norm of this Vector.
   */
  implicit val canNorm: norm.Impl[BreezePSVector, Double] = {
    new norm.Impl[BreezePSVector, Double] {
      def apply(v: BreezePSVector): Double = {
        psClient.vectorOps.nrm2(v)
      }
    }
  }

  /**
   * Returns the p-norm of this Vector.
   */
  implicit val canNorm2: norm.Impl2[BreezePSVector, Double, Double] = {
    new norm.Impl2[BreezePSVector, Double, Double] {
      def apply(v: BreezePSVector, p: Double): Double = {
        if (p == 2) {
          psClient.vectorOps.nrm2(v)
        } else if (p == 1) {
          psClient.vectorOps.asum(v)
        } else if (p == Double.PositiveInfinity) {
          psClient.vectorOps.amax(v)
        } else if (p == 0) {
          psClient.vectorOps.nnz(v)
        } else {
          throw new SparkException("Dose not support p-norms other than L0, L1, L2 and Linf")
        }
      }
    }
  }

  implicit val canDim: dim.Impl[BreezePSVector, Int] = new dim.Impl[BreezePSVector, Int] {
    def apply(v: BreezePSVector): Int = v.dimension.toInt
  }

  implicit val space: MutableLPVectorField[BreezePSVector, Double] = {
    MutableLPVectorField.make[BreezePSVector, Double]
  }

  def psClient = PSClient.instance()

  // used to make sure the operators are loaded
  @noinline
  private def init() = {

  }
}
