package com.tencent.angel.ml.math.vector

import com.tencent.angel.ml.math.TVector

abstract class TDoubleVector extends TVector{

  def this(other: TDoubleVector)={
    this()
    this.rowId = other.getRowId
    this.matrixId = other.getMatrixId
    this.clock = other.getClock
    this.dim = other.getDimension
  }

  def get(index:Long):Double = ???

  def get(index:Int):Double = ???

  def set(index: Int, value: Double):Unit = ???

  def set(index: Long, value: Double):Unit = ???

  def squaredNorm: Double

  def norm(): Double
}
