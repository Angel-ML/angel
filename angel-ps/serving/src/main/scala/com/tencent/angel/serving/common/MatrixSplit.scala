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

package com.tencent.angel.serving.common

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math.matrix._
import com.tencent.angel.ml.math.vector.{SparseDoubleVector, SparseFloatVector, SparseIntVector, SparseLongKeyDoubleVector}
import com.tencent.angel.ml.math.{TMatrix, TVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.model.output.format.ModelPartitionMeta
import com.tencent.angel.serving.ShardingMatrix
import com.tencent.angel.tools.ModelLoader
import it.unimi.dsi.fastutil.ints.{Int2DoubleOpenHashMap, Int2FloatOpenHashMap, Int2IntOpenHashMap}
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * the matrix split represent a uniform calculate unit for serving
  *
  * @param name         the matrix name
  * @param idx          the index of split
  * @param rowOffset    the row offset
  * @param rowNum       the row num
  * @param columnOffset the column offset
  * @param dimension    the dimension
  */
case class MatrixSplit(val name: String, val idx: Int, val rowOffset: Int, val rowNum: Int, val columnOffset: Long, val dimension: Long) {

  /**
    * load {@link ShardingMatrix} from model directory
    *
    * @param modelDir
    * @param config
    * @tparam V
    * @return
    */
  def load[V](modelDir: String, config: Configuration): ShardingMatrix = {
    val meta = ModelLoader.getMeta(new Path(modelDir, name).toString, config)
    val columnLimit = columnOffset + dimension
    val rowLimit = rowOffset + rowNum
    val fs = new Path(modelDir).getFileSystem(config)
    val matrixDir = new Path(modelDir, name)
    val refParts = meta.getPartMetas.asScala.filter {
      case (_, partition) => partition.getStartRow < rowLimit && partition.getEndRow >= rowOffset && partition.getStartCol < columnLimit && partition.getEndCol >= columnOffset
    }.map { case (_, partition) => partition }

    val partMetaStream = refParts.toStream.sortBy(part => (part.getStartRow, part.getStartCol, part.getEndRow, part.getEndCol))


    val inputs = new mutable.HashMap[String, FSDataInputStream]()

    def toBlock(partMeta: ModelPartitionMeta): (Long, Long, Long, Long) = {
      (partMeta.getStartRow,
        partMeta.getEndRow,
        partMeta.getStartCol,
        partMeta.getEndCol)
    }

    def toInt(block: (Long, Long, Long, Long)): (Int, Int, Int, Int) = block match {
      case (l1, l2, l3, l4) =>
        require(l1 <= Int.MaxValue && l2 <= Int.MaxValue && l3 <= Int.MaxValue && l4 <= Int.MaxValue)
        (l1.toInt, l2.toInt, l3.toInt, l4.toInt)
    }

    def isSurround(columnId: Int): Boolean = {
      columnId >= columnOffset && columnId < columnLimit
    }

    var matrix: TMatrix[_ <: TVector] = null

    RowType.valueOf(meta.getRowType) match {
      case RowType.T_DOUBLE_DENSE => {
        val rowMap = new mutable.HashMap[Int, Array[Double]]()
        val (rNum, cNum) = {
          require(rowNum <= Int.MaxValue && dimension <= Int.MaxValue)
          (rowNum.toInt, dimension.toInt)
        }
        partMetaStream.foreach(partMeta => {
          val (rowStart, rowEnd, columnStart, columnEnd) = toInt(toBlock(partMeta))
          val input = inputs.getOrElseUpdate(partMeta.getFileName, fs.open(new Path(matrixDir, partMeta.getFileName)))
          (rowStart to rowEnd).map(rowId => (rowId, ModelLoader.loadDenseDoubleRowFromPartition(input, partMeta, rowId)))
            .foreach {
              case (rowId, row) => {
                val baseRow = rowMap.getOrElseUpdate(rowId, new Array[Double](cNum))
                Array.copy(row, columnStart, baseRow, columnStart, (columnEnd - columnStart))
              }
            }
        })
        matrix = new DenseDoubleMatrix(rNum, cNum, rowMap.toStream.sortBy { case (index, _) => index }.map { case (_, row) => row }.toArray)
      }

      case RowType.T_DOUBLE_SPARSE | RowType.T_DOUBLE_SPARSE_COMPONENT => {
        val rowMap = new mutable.HashMap[Int, Int2DoubleOpenHashMap]()
        val (rNum, cNum) = {
          require(rowNum <= Int.MaxValue && dimension <= Int.MaxValue)
          (rowNum.toInt, dimension.toInt)
        }
        partMetaStream.foreach(partMeta => {
          val (rowStart, rowEnd, columnStart, columnEnd) = toInt(toBlock(partMeta))
          val surround = columnStart >= columnOffset && columnEnd <= (columnOffset + dimension)
          val input = inputs.getOrElseUpdate(partMeta.getFileName, fs.open(new Path(matrixDir, partMeta.getFileName)))
          (rowStart until rowEnd).map(rowId => (rowId, ModelLoader.loadSparseDoubleRowFromPartition(input, partMeta, rowId)))
            .foreach {
              case (rowId, row) => {
                val baseRow = rowMap.getOrElseUpdate(rowId, new Int2DoubleOpenHashMap())
                if (surround) {
                  baseRow.putAll(row)
                } else {
                  baseRow.putAll(row.asScala.filterKeys(key => isSurround(key)).asJava)
                }
              }
            }
          matrix = new SparseDoubleMatrix(rNum, cNum,
            rowMap.toStream.sortBy { case (index, _) => index }.map { case (_, row) => new SparseDoubleVector(cNum, row) }.toArray)
        })
      }
      case RowType.T_DOUBLE_SPARSE_LONGKEY | RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT => {
        def isSurround(columnId: Long): Boolean = {
          columnId >= columnOffset && columnId <= columnLimit
        }

        val rowMap = new mutable.HashMap[Int, Long2DoubleOpenHashMap]()
        partMetaStream.foreach(partMeta => {
          val (rowStart, rowEnd, columnStart, columnEnd) = toInt(toBlock(partMeta))
          val surround = columnStart >= columnOffset && columnEnd <= (columnOffset + dimension)
          val input = inputs.getOrElseUpdate(partMeta.getFileName, fs.open(new Path(matrixDir, partMeta.getFileName)))
          (rowStart until rowEnd).map(rowId => (rowId, ModelLoader.loadSparseDoubleLongKeyRowFromPartition(input, partMeta, rowId)))
            .foreach {
              case (rowId, row) => {
                val baseRow = rowMap.getOrElseUpdate(rowId, new Long2DoubleOpenHashMap())
                if (surround) {
                  baseRow.putAll(row)
                } else {
                  baseRow.putAll(row.asScala.filterKeys(key => isSurround(key)).asJava)
                }
              }
            }
        })
        matrix = new SparseDoubleLongKeyMatrix(rowNum.intValue(), dimension,
          rowMap.toStream.sortBy { case (index, _) => index }.map { case (_, row) => new SparseLongKeyDoubleVector(dimension, row) }.toArray)
      }

      case RowType.T_FLOAT_DENSE
      => {
        val rowMap = new mutable.HashMap[Int, Array[Float]]()
        val (rNum, cNum) = {
          require(rowNum <= Int.MaxValue && dimension <= Int.MaxValue)
          (rowNum.toInt, dimension.toInt)
        }
        partMetaStream.foreach(partMeta => {
          val (rowStart, rowEnd, columnStart, columnEnd) = toInt(toBlock(partMeta))
          val input = inputs.getOrElseUpdate(partMeta.getFileName, fs.open(new Path(matrixDir, partMeta.getFileName)))
          (rowStart until rowEnd).map(rowId => (rowId, ModelLoader.loadDenseFloatRowFromPartition(input, partMeta, rowId)))
            .foreach {
              case (rowId, row) => {
                val baseRow = rowMap.getOrElseUpdate(rowId, new Array[Float](cNum))
                Array.copy(row, columnStart, baseRow, columnStart, (columnEnd - columnStart))
              }
            }
        })
        matrix = new DenseFloatMatrix(rNum, cNum, rowMap.toStream.sortBy { case (index, _) => index }.map { case (_, row) => row }.toArray)
      }
      case RowType.T_FLOAT_SPARSE | RowType.T_FLOAT_SPARSE_COMPONENT
      => {
        val rowMap = new mutable.HashMap[Int, Int2FloatOpenHashMap]()
        val (rNum, cNum) = {
          require(rowNum <= Int.MaxValue && dimension <= Int.MaxValue)
          (rowNum.toInt, dimension.toInt)
        }
        partMetaStream.foreach(partMeta => {
          val (rowStart, rowEnd, columnStart, columnEnd) = toInt(toBlock(partMeta))
          val surround = columnStart >= columnOffset && columnEnd <= (columnOffset + dimension)
          val input = inputs.getOrElseUpdate(partMeta.getFileName, fs.open(new Path(matrixDir, partMeta.getFileName)))
          (rowStart until rowEnd).map(rowId => (rowId, ModelLoader.loadSparseFloatRowFromPartition(input, partMeta, rowId)))
            .foreach {
              case (rowId, row) => {
                val baseRow = rowMap.getOrElseUpdate(rowId, new Int2FloatOpenHashMap())
                if (surround) {
                  baseRow.putAll(row)
                } else {
                  baseRow.putAll(row.asScala.filterKeys(key => isSurround(key)).asJava)
                }
              }
            }
          matrix = new SparseFloatMatrix(rNum, cNum,
            rowMap.toStream.sortBy { case (index, _) => index }.map { case (_, row) => new SparseFloatVector(cNum, row) }.toArray)
        })
      }

      case RowType.T_INT_DENSE
      => {
        val rowMap = new mutable.HashMap[Int, Array[Int]]()
        val (rNum, cNum) = {
          require(rowNum <= Int.MaxValue && dimension <= Int.MaxValue)
          (rowNum.toInt, dimension.toInt)
        }
        partMetaStream.foreach(partMeta => {
          val (rowStart, rowEnd, columnStart, columnEnd) = toInt(toBlock(partMeta))
          val input = inputs.getOrElseUpdate(partMeta.getFileName, fs.open(new Path(matrixDir, partMeta.getFileName)))
          (rowStart until rowEnd).map(rowId => (rowId, ModelLoader.loadDenseFloatRowFromPartition(input, partMeta, rowId)))
            .foreach {
              case (rowId, row) => {
                val baseRow = rowMap.getOrElseUpdate(rowId, new Array[Int](cNum))
                Array.copy(row, columnStart, baseRow, columnStart, (columnEnd - columnStart))
              }
            }
        })
        matrix = new DenseIntMatrix(rNum, cNum, rowMap.toStream.sortBy { case (index, _) => index }.map { case (_, row) => row }.toArray)
      }
      case RowType.T_INT_SPARSE | RowType.T_INT_SPARSE_COMPONENT
      => {
        val rowMap = new mutable.HashMap[Int, Int2IntOpenHashMap]()
        val (rNum, cNum) = {
          require(rowNum <= Int.MaxValue && dimension <= Int.MaxValue)
          (rowNum.toInt, dimension.toInt)
        }
        partMetaStream.foreach(partMeta => {
          val (rowStart, rowEnd, columnStart, columnEnd) = toInt(toBlock(partMeta))
          val surround = columnStart >= columnOffset && columnEnd <= (columnOffset + dimension)
          val input = inputs.getOrElseUpdate(partMeta.getFileName, fs.open(new Path(matrixDir, partMeta.getFileName)))
          (rowStart until rowEnd).map(rowId => (rowId, ModelLoader.loadSparseIntRowFromPartition(input, partMeta, rowId)))
            .foreach {
              case (rowId, row) => {
                val baseRow = rowMap.getOrElseUpdate(rowId, new Int2IntOpenHashMap())
                if (surround) {
                  baseRow.putAll(row)
                } else {
                  baseRow.putAll(row.asScala.filterKeys(key => isSurround(key)).asJava)
                }
              }
            }
          matrix = new SparseIntMatrix(rNum, cNum,
            rowMap.toStream.sortBy { case (index, _) => index }.map { case (_, row) => new SparseIntVector(cNum, row) }.toArray)
        })
      }

      case _ => throw new AngelException("unsupported")
    }
    inputs.foreach { case (_, input) => input.close() }
    new ShardingMatrix(matrix.asInstanceOf[TMatrix[TVector]], rowOffset, columnOffset)
  }
}
