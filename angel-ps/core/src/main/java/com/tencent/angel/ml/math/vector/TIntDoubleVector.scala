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
package com.tencent.angel.ml.math.vector

/**
  * Base class of double vector
  */
abstract class TIntDoubleVector() extends TDoubleVector {

  def this(other:TIntDoubleVector){
    this()
    this.rowId = other.getRowId
    this.matrixId = other.getMatrixId
    this.clock = other.getClock
    this.dim = other.getDimension
  }
  /**
    * Get all indexes of vector
    *
    * @return all indexes of vector
    */
  def getIndices: Array[Int]

  /**
    * Get all values of vector
    *
    * @return all values of vector
    */
  def getValues: Array[Double]

  /**
    * Get a element value
    *
    * @param index element index
    * @return element value
    */
  def get(index: Int): Double

  /**
    * Set a element value
    *
    * @param index element index
    * @param value element value
    */
  def set(index: Int, value: Double)


  /**
    * Clone vector
    *
    * @return cloned vector
    */
  override def clone: TIntDoubleVector = throw new CloneNotSupportedException

  /**
    * Plus a element by a update value
    *
    * @param index element value
    * @param delta update value
    * @return this
    */
  def plusBy(index: Int, delta: Double): TIntDoubleVector

  def sum: Double
}
