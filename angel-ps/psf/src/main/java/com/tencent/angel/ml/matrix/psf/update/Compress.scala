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


package com.tencent.angel.ml.matrix.psf.update

import scala.collection.JavaConversions._
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.utils.VectorUtils
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector._

class Compress(param: MMUpdateParam) extends MMUpdateFunc(param) {
  val defaultFilterLimit = 1e-11

  def this(matrixId: Int, rowId: Int) =
    this(new MMUpdateParam(matrixId, Array[Int](rowId), Array[Double]()))

  def this() = this(null)

  override protected def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    rows.foreach {
      case r: ServerLongDoubleRow => {
        try {
          r.startWrite()
          r.setSplit(filterLongDoubleVec(r))
        } finally {
          r.endWrite()
        }
      }

      case r: ServerLongFloatRow => {
        try {
          r.startWrite()
          r.setSplit(filterLongFloatVec(r))
        } finally {
          r.endWrite()
        }
      }

      case r: ServerLongLongRow => {
        try {
          r.startWrite()
          r.setSplit(filterLongLongVec(r))
        } finally {
          r.endWrite()
        }
      }

      case r: ServerLongIntRow => {
        try {
          r.startWrite()
          r.setSplit(filterLongIntVec(r))
        } finally {
          r.endWrite()
        }
      }

      case r => throw new AngelException(s"not implemented for ${r.getRowType}")
    }
  }

  def filterLongDoubleVec(r: ServerLongDoubleRow) : DoubleVector = {
    var newSplit: DoubleVector = null
    newSplit = VectorUtils.emptyLike(r.getSplit).asInstanceOf[DoubleVector]
    r.getSplit match {
      case intDoubleVec: IntDoubleVector => {
        if(intDoubleVec.isDense) {
          val data = intDoubleVec.getStorage.getValues
          for((value, index) <- data.zipWithIndex) {
            if (math.abs(value) > defaultFilterLimit) newSplit.asInstanceOf[IntDoubleVector].set(index, value)
          }
        } else {
          val iter = intDoubleVec.getStorage.entryIterator()
          iter.foreach {
            entry => if (math.abs(entry.getDoubleValue) > defaultFilterLimit) newSplit.asInstanceOf[IntDoubleVector].set(entry.getIntKey, entry.getDoubleValue)
          }
        }
      }

      case longDoubleVec: LongDoubleVector => {
        if(longDoubleVec.isDense) {
          val data = longDoubleVec.getStorage.getValues
          for((value, index) <- data.zipWithIndex) {
            if (math.abs(value) > defaultFilterLimit) newSplit.asInstanceOf[LongDoubleVector].set(index, value)
          }
        } else {
          val iter = longDoubleVec.getStorage.entryIterator()
          iter.foreach {
            entry => if (math.abs(entry.getDoubleValue) > defaultFilterLimit) newSplit.asInstanceOf[LongDoubleVector].set(entry.getLongKey, entry.getDoubleValue)
          }
        }
      }
    }
    newSplit
  }

  def filterLongFloatVec(r: ServerLongFloatRow) : FloatVector = {
    var newSplit: FloatVector = null
    r.startRead()
    try {
      newSplit = VectorUtils.emptyLike(r.getSplit).asInstanceOf[FloatVector]
      r.getSplit match {
        case intFloatVec: IntFloatVector => {
          if(intFloatVec.isDense) {
            val data = intFloatVec.getStorage.getValues
            for((value, index) <- data.zipWithIndex) {
              if (math.abs(value) > defaultFilterLimit) newSplit.asInstanceOf[IntFloatVector].set(index, value)
            }
          } else {
            val iter = intFloatVec.getStorage.entryIterator()
            iter.foreach {
              entry => if (math.abs(entry.getFloatValue) > defaultFilterLimit) newSplit.asInstanceOf[IntDoubleVector].set(entry.getIntKey, entry.getFloatValue)
            }
          }
        }

        case longFloatVec: LongFloatVector => {
          if(longFloatVec.isDense) {
            val data = longFloatVec.getStorage.getValues
            for((value, index) <- data.zipWithIndex) {
              if (math.abs(value) > defaultFilterLimit) newSplit.asInstanceOf[LongFloatVector].set(index, value)
            }
          } else {
            val iter = longFloatVec.getStorage.entryIterator()
            iter.foreach {
              entry => if (math.abs(entry.getFloatValue) > defaultFilterLimit) newSplit.asInstanceOf[LongFloatVector].set(entry.getLongKey, entry.getFloatValue)
            }
          }
        }
      }
    } finally {
      r.endRead()
    }

    newSplit
  }

  def filterLongIntVec(r: ServerLongIntRow) : IntVector = {
    var newSplit: IntVector = null
    r.startRead()
    try {
      newSplit = VectorUtils.emptyLike(r.getSplit).asInstanceOf[IntVector]
      r.getSplit match {
        case intIntVec: IntIntVector => {
          if(intIntVec.isDense) {
            val data = intIntVec.getStorage.getValues
            for((value, index) <- data.zipWithIndex) {
              if (math.abs(value) > defaultFilterLimit) newSplit.asInstanceOf[IntIntVector].set(index, value)
            }
          } else {
            val iter = intIntVec.getStorage.entryIterator()
            iter.foreach {
              entry => if (math.abs(entry.getIntValue) > defaultFilterLimit) newSplit.asInstanceOf[IntDoubleVector].set(entry.getIntKey, entry.getIntValue)
            }
          }
        }

        case longIntVec: LongIntVector => {
          if(longIntVec.isDense) {
            val data = longIntVec.getStorage.getValues
            for((value, index) <- data.zipWithIndex) {
              if (math.abs(value) > defaultFilterLimit) newSplit.asInstanceOf[LongIntVector].set(index, value)
            }
          } else {
            val iter = longIntVec.getStorage.entryIterator()
            iter.foreach {
              entry => if (math.abs(entry.getIntValue) > defaultFilterLimit) newSplit.asInstanceOf[LongIntVector].set(entry.getLongKey, entry.getIntValue)
            }
          }
        }
      }
    } finally {
      r.endRead()
    }

    newSplit
  }

  def filterLongLongVec(r: ServerLongLongRow) : LongVector = {
    var newSplit: LongVector = null
    r.startRead()
    try {
      newSplit = VectorUtils.emptyLike(r.getSplit).asInstanceOf[LongVector]
      r.getSplit match {
        case intLongVec: IntLongVector => {
          if(intLongVec.isDense) {
            val data = intLongVec.getStorage.getValues
            for((value, index) <- data.zipWithIndex) {
              if (math.abs(value) > defaultFilterLimit) newSplit.asInstanceOf[IntLongVector].set(index, value)
            }
          } else {
            val iter = intLongVec.getStorage.entryIterator()
            iter.foreach {
              entry => if (math.abs(entry.getLongValue) > defaultFilterLimit) newSplit.asInstanceOf[IntLongVector].set(entry.getIntKey, entry.getLongValue)
            }
          }
        }

        case longLongVec: LongLongVector => {
          if(longLongVec.isDense) {
            val data = longLongVec.getStorage.getValues
            for((value, index) <- data.zipWithIndex) {
              if (math.abs(value) > defaultFilterLimit) newSplit.asInstanceOf[LongLongVector].set(index, value)
            }
          } else {
            val iter = longLongVec.getStorage.entryIterator()
            iter.foreach {
              entry => if (math.abs(entry.getLongValue) > defaultFilterLimit) newSplit.asInstanceOf[LongLongVector].set(entry.getLongKey, entry.getLongValue)
            }
          }
        }
      }
    } finally {
      r.endRead()
    }

    newSplit
  }
}
