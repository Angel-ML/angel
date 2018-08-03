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
package com.tencent.angel.ml.math

import com.tencent.angel.ml.math.vector._


/**
  * The vector oriented column, represented a one-dimensional values.
  */
abstract class TVector() extends TAbstractVector {

  def this(other: TVector)={
    this()
    this.rowId = other.getRowId
    this.matrixId = other.getMatrixId
    this.clock = other.getClock
    this.dim = other.getDimension
  }


  /**
    * Plus this vector with other vector element by element.
    *
    * @param other the other vector
    * @return the reference of object
    */
  def plusBy(other: TAbstractVector): TVector

  /**
    * Plus a element of vector.
    *
    * @param index the element index
    * @param x     the double update value
    * @return the reference of object
    */
  def plusBy(index: Int, x: Double): TVector

  /**
    * Multiply plus this vector with other vector element by element.
    *
    * @param other the other
    * @param x     the double multiply factor
    * @return the reference of object
    */
  def plusBy(other: TAbstractVector, x: Double): TVector

  /**
    * Plus this vector with other vector to generate a new vector element by element.
    *
    * @param other the other vector
    * @return the new vector
    */
  def plus(other: TAbstractVector): TVector

  /**
    * Multiply plus this vector with other vector to a generate new vector element by element.
    *
    * @param other the other
    * @param x     the double multiply factor
    * @return the new vector
    */
  def plus(other: TAbstractVector, x: Double): TVector

  /**
    * Dot this vector with other.
    *
    * @param other the other
    * @return the result
    */
  def dot(other: TAbstractVector): Double

  /**
    * Times this vector with x, generating a new vector
    *
    * @param x the double multiply factor
    * @return the new vector
    */
  def times(x: Double): TVector

  /**
    * Times this vector with x
    *
    * @param x the double multiply factor
    * @return the reference of object
    */
  def timesBy(x: Double): TVector

  /**
    * Filter value whose absolute value less than x.
    *
    * @param x the comparison value
    * @return the t vector
    */
  def filter(x: Double): TVector

  /**
    * Clear.
    */
  def clear()

  /**
    * Non zero number long.
    *
    * @return the long
    */
  def nonZeroNumber(): Long

  /**
    * Calculate square norm value of vector
    *
    * @return square norm value
    */
  def squaredNorm(): Double

  /**
    * Clone.
    *
    * @param vector for cloning the vector
    */
  def clone(vector: TVector) {
    assert(this.getClass eq vector.getClass)
    assert(this.dim == vector.dim)
    this.matrixId = vector.matrixId
    this.rowId = vector.rowId
    this.clock = vector.clock
    this.dim = vector.dim
  }

  override def clone: TVector = throw new CloneNotSupportedException
}

object TVector{
  import VectorType._
  def apply(dims: Int, vtype:VectorType):TVector ={
    vtype match {
      case T_DOUBLE_DENSE => new DenseDoubleVector(dims)
      case T_DOUBLE_SPARSE => new SparseDoubleVector(dims)
      case T_INT_DENSE => new DenseIntVector(dims)
      case T_INT_SPARSE =>new SparseIntVector(dims)
    }
  }

  def apply(dims: Long, vType:VectorType):TVector = {
    vType match {
      case T_DOUBLE_SPARSE_LONGKEY => new SparseLongKeyDoubleVector(dims)
    }
  }
}
