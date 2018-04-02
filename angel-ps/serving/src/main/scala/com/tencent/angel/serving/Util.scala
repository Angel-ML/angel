/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/BSD-3-Clause
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package com.tencent.angel.serving

import java.io._
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path


object Util {
  def initVector[V <: TVector](rowType: RowType, dim: Long): V = {
    rowType match {
      case RowType.T_DOUBLE_DENSE =>
        new DenseDoubleVector(dim.toInt).asInstanceOf[V]
      case RowType.T_DOUBLE_SPARSE =>
        new SparseDoubleVector(dim.toInt).asInstanceOf[V]
      case RowType.T_DOUBLE_SPARSE_LONGKEY =>
        new SparseLongKeyDoubleVector(dim).asInstanceOf[V]
      case RowType.T_FLOAT_DENSE =>
        new DenseFloatVector(dim.toInt).asInstanceOf[V]
      case RowType.T_FLOAT_SPARSE =>
        new SparseFloatVector(dim.toInt).asInstanceOf[V]
      case RowType.T_INT_DENSE =>
        new DenseIntVector(dim.toInt).asInstanceOf[V]
      case RowType.T_INT_SPARSE =>
        new SparseIntVector(dim.toInt).asInstanceOf[V]
      case _ =>
        throw new AngelException(s"unsupported $rowType")
    }
  }

  def read(conf: Configuration, dir: Path, dim: Long, format: String = "dummy"): Iterator[TVector] = {
    val input = dir.getFileSystem(conf).open(dir)
    val parser = DataParser.apply(conf)
    val reader = new BufferedReader(new InputStreamReader(input))

    //TODO support train data
    def parse(vector: TVector, format: String): TVector = {
      format match {
        case "dummy" => {
          val dummyVector = vector.asInstanceOf[SparseDummyVector]
          val indices = dummyVector.getIndices
          new SparseDoubleVector(dummyVector.getDimension, indices, (0 until indices.length).map(_ => 1.0).toArray)
        }
        case "libsvm" => {
          val dummyVector = vector.asInstanceOf[SparseDoubleSortedVector]
          new SparseDoubleVector(dummyVector.getDimension, dummyVector.getIndices, dummyVector.getValues)
        }
        case "dummy-64" => {
          val dummyVector = vector.asInstanceOf[SparseLongKeyDummyVector]
          val indices = dummyVector.getIndexes
          new SparseLongKeyDoubleVector(dummyVector.getLongDim, indices, (0 until indices.length).map(_ => 1.0).toArray)
        }
        case "libsvm-64" => {
          val dummyVector = vector.asInstanceOf[SparseLongKeySortedDoubleVector]
          new SparseLongKeyDoubleVector(dummyVector.getDimension, dummyVector.getIndexes, dummyVector.getValues)
        }
      }
    }

    new Iterator[TVector] {
      var vector: TVector = null
      var eof = false

      override def hasNext = {
        if (eof) {
          false
        } else {
          if (vector == null) {
            val data = reader.readLine()
            if (data == null) {
              eof = true
            } else {
              val labeledData = parser.parse(data)
              vector = parse(labeledData.getX, format)
            }
          }
          if (vector != null) true else false
        }
      }


      override def next() = {
        require(vector != null)
        val next = vector
        vector = null
        next
      }
    }
  }

  /**
    * summary offsets's represented length
    *
    * @param offsets (pos,offset)
    * @return
    */
  def sum(offsets: Array[(Int, Int)]): Int = {
    // pos as asc ,offset as desc
    val order = new Ordering[(Int, Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)) = {
        if (x._1 == y._1) {
          y._2.compareTo(x._2)
        } else {
          x._1.compareTo(y._1)
        }
      }
    }
    val newOffsets = offsets.sorted(order)
    var curOffset: (Int, Int) = newOffsets(0)
    var curLimit = curOffset._1 + curOffset._2
    var length = curOffset._2
    for (offset <- newOffsets.slice(1, newOffsets.length)) {
      if ((offset._1 + offset._2) > curLimit) {
        //intersect
        if (offset._1 < curLimit) {
          length += offset._2 - (curLimit - offset._1)
        } else {
          length += offset._2
        }
        curOffset = offset
        curLimit = curOffset._1 + curOffset._2
      }
    }
    length
  }


  /**
    * summary offsets's represented length
    *
    * @param offsets (pos,offset)
    * @return
    */
  def sum(offsets: Array[(Long, Long)]): Long = {
    // pos as asc ,offset as desc
    val order = new Ordering[(Long, Long)] {
      override def compare(x: (Long, Long), y: (Long, Long)) = {
        if (x._1 == y._1) {
          y._2.compareTo(x._2)
        } else {
          x._1.compareTo(y._1)
        }
      }
    }
    val newOffsets = offsets.sorted(order)
    var curOffset: (Long, Long) = newOffsets(0)
    var curLimit = curOffset._1 + curOffset._2
    var length = curOffset._2
    for (offset <- newOffsets.slice(1, newOffsets.length)) {
      if ((offset._1 + offset._2) > curLimit) {
        //intersect
        if (offset._1 < curLimit) {
          length += offset._2 - (curLimit - offset._1)
        } else {
          length += offset._2
        }
        curOffset = offset
        curLimit = curOffset._1 + curOffset._2
      }
    }
    length
  }


  def createThreadFactory(namePrefix: String) = new NamedThreadFactory(namePrefix)
}

class NamedThreadFactory(val namePrefix: String) extends ThreadFactory {
  private val threadNumber = new AtomicInteger(1)

  override def newThread(r: Runnable): Thread = {
    val t = new Thread(r)
    t.setName(namePrefix + threadNumber.getAndIncrement)
    if (t.isDaemon) t.setDaemon(false)
    if (t.getPriority != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY)
    t
  }
}
