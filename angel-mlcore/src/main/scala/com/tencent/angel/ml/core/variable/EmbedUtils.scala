package com.tencent.angel.ml.core.variable

import java.lang.{Long => JLong}
import java.util.{HashMap => JHashMap, Map => JMap}

import com.tencent.angel.ml.core.network.{Graph, PlaceHolder}
import com.tencent.angel.ml.core.utils.MLException
import com.tencent.angel.ml.servingmath2.matrix.{MapMatrix, Matrix}
import com.tencent.angel.ml.servingmath2.storage._
import com.tencent.angel.ml.servingmath2.vector._
import com.tencent.angel.ml.servingmath2.{MFactory, VFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object EmbedUtils {
  val OneHot = "oneHot"
  val MultiHot = "multiHot"

  def geneMatrix(placeHolder: PlaceHolder, assembleHint: String,
                 embeddings: JMap[JLong, Vector]): (Matrix, List[mutable.HashMap[Int, Int]]) = {
    val features = placeHolder.getFeats

    val arrBuf = new ArrayBuffer[mutable.HashMap[Int, Int]]()
    val rows = (0 until features.getNumRows).toArray.map { rId =>
      val row = features.getRow(rId)
      val partitions = if (assembleHint == MultiHot && placeHolder.hasFieldKeyMap) {
        val fieldKeyMap = placeHolder.getFieldKeyMap
        assert(fieldKeyMap != null, "fieldKeyMap cannot be null")
        val (parts, stats) = multiHotAssemble(row, embeddings, fieldKeyMap)
        arrBuf.append(stats)
        parts
      } else {
        val (parts, stats) = oneHotAssemble(row, embeddings)
        if (stats != null) {
          arrBuf.append(stats)
        }
        parts
      }

      partitions.head match {
        case v: IntDoubleVector =>
          VFactory.compIntDoubleVector(
            v.dim().toInt * partitions.length, partitions.map(_.asInstanceOf[IntDoubleVector]), v.dim().toInt
          )
        case v: IntFloatVector =>
          VFactory.compIntFloatVector(
            v.dim().toInt * partitions.length, partitions.map(_.asInstanceOf[IntFloatVector]), v.dim().toInt
          )
        case _ => throw MLException("Vector type is not supported!")
      }
    }

    if (rows.head.getType.isDouble) {
      MFactory.rbCompIntDoubleMatrix(rows.map(_.asInstanceOf[CompIntDoubleVector])) -> arrBuf.toList
    } else if (rows.head.getType.isFloat) {
      MFactory.rbCompIntFloatMatrix(rows.map(_.asInstanceOf[CompIntFloatVector])) -> arrBuf.toList
    } else {
      throw MLException("Vector type is not supported!")
    }
  }

  def oneHotAssemble(row: Vector, embeddings: JMap[JLong, Vector]): (Array[Vector], mutable.HashMap[Int, Int]) = {
    val partitions = row.getStorage match {
      case s: IntDoubleDenseVectorStorage =>
        s.getValues.zipWithIndex.map { case (value, idx) =>
          val vec = embeddings.get(idx.toLong)
          if (value == 1) {
            vec
          } else {
            vec.mul(value)
          }
        }
      case s: IntDoubleSparseVectorStorage =>
        s.getIndices.sorted.map { idx =>
          val vec = embeddings.get(idx.toLong)
          val value = s.get(idx)
          if (value == 1) {
            vec
          } else {
            vec.mul(value)
          }
        }
      case s: IntDoubleSortedVectorStorage =>
        s.getValues.zip(s.getIndices).map { case (value, idx) =>
          val vec = embeddings.get(idx.toLong)
          if (value == 1) {
            vec
          } else {
            vec.mul(value)
          }
        }
      case s: LongDoubleSparseVectorStorage =>
        s.getIndices.sorted.map { idx =>
          val vec = embeddings.get(idx)
          val value = s.get(idx)
          if (value == 1) {
            vec
          } else {
            vec.mul(value)
          }
        }
      case s: LongDoubleSortedVectorStorage =>
        s.getValues.zip(s.getIndices).map { case (value, idx) =>
          val vec = embeddings.get(idx)
          if (value == 1) {
            vec
          } else {
            vec.mul(value)
          }
        }
      case s: IntFloatDenseVectorStorage =>
        s.getValues.zipWithIndex.map { case (value, idx) =>
          val vec = embeddings.get(idx.toLong)
          if (value == 1) {
            vec
          } else {
            vec.mul(value)
          }
        }
      case s: IntFloatSparseVectorStorage =>
        s.getIndices.sorted.map { idx =>
          val vec = embeddings.get(idx.toLong)
          val value = s.get(idx)
          if (value == 1) {
            vec
          } else {
            vec.mul(value)
          }
        }
      case s: IntFloatSortedVectorStorage =>
        s.getValues.zip(s.getIndices).map { case (value, idx) =>
          val vec = embeddings.get(idx.toLong)
          if (value == 1) {
            vec
          } else {
            vec.mul(value)
          }
        }
      case s: LongFloatSparseVectorStorage =>
        s.getIndices.sorted.map { idx =>
          val vec = embeddings.get(idx)
          val value = s.get(idx)
          if (value == 1) {
            vec
          } else {
            vec.mul(value)
          }
        }
      case s: LongFloatSortedVectorStorage =>
        s.getValues.zip(s.getIndices).map { case (value, idx) =>
          val vec = embeddings.get(idx)
          if (value == 1) {
            vec
          } else {
            vec.mul(value)
          }
        }
    }

    partitions -> null
  }

  def multiHotAssemble(row: Vector, embeddings: JMap[JLong, Vector], fieldKeyMap: Map[Long, Int]): (Array[Vector], mutable.HashMap[Int, Int]) = {
    val stats = new mutable.HashMap[Int, Int]()
    val vectorMap = new mutable.HashMap[Int, Vector]()

    row.getStorage match {
      case s: IntDoubleDenseVectorStorage =>
        s.getValues.zipWithIndex.foreach { case (value, idx) =>
          val field = fieldKeyMap(idx.toLong)
          if (value == 1) {
            val vec = embeddings.get(idx.toLong)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec.copy)
            }
          } else {
            val vec = embeddings.get(idx.toLong).mul(value)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec)
            }
          }
        }
      case s: IntDoubleSparseVectorStorage =>
        s.getIndices.foreach { idx =>
          val value = s.get(idx)
          val field = fieldKeyMap(idx.toLong)
          if (value == 1) {
            val vec = embeddings.get(idx.toLong)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec.copy)
            }
          } else {
            val vec = embeddings.get(idx.toLong).mul(value)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec)
            }
          }
        }
      case s: IntDoubleSortedVectorStorage =>
        s.getValues.zip(s.getIndices).foreach { case (value, idx) =>
          val field = fieldKeyMap(idx.toLong)
          if (value == 1) {
            val vec = embeddings.get(idx.toLong)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec.copy)
            }
          } else {
            val vec = embeddings.get(idx.toLong).mul(value)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec)
            }
          }
        }
      case s: LongDoubleSparseVectorStorage =>
        s.getIndices.foreach { idx =>
          val value = s.get(idx)
          val field = fieldKeyMap(idx)
          if (value == 1) {
            val vec = embeddings.get(idx)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec.copy)
            }
          } else {
            val vec = embeddings.get(idx).mul(value)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec)
            }
          }
        }
      case s: LongDoubleSortedVectorStorage =>
        s.getValues.zip(s.getIndices).foreach { case (value, idx) =>
          val field = fieldKeyMap(idx)
          if (value == 1) {
            val vec = embeddings.get(idx)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec.copy)
            }
          } else {
            val vec = embeddings.get(idx).mul(value)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec)
            }
          }
        }
      case s: IntFloatDenseVectorStorage =>
        s.getValues.zipWithIndex.foreach { case (value, idx) =>
          val field = fieldKeyMap(idx)
          if (value == 1) {
            val vec = embeddings.get(idx.toLong)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec.copy)
            }
          } else {
            val vec = embeddings.get(idx.toLong).mul(value)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec)
            }
          }
        }
      case s: IntFloatSparseVectorStorage =>
        s.getIndices.foreach { idx =>
          val value = s.get(idx)
          val field = fieldKeyMap(idx)
          if (value == 1) {
            val vec = embeddings.get(idx.toLong)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec.copy)
            }
          } else {
            val vec = embeddings.get(idx.toLong).mul(value)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec)
            }
          }
        }
      case s: IntFloatSortedVectorStorage =>
        s.getValues.zip(s.getIndices).foreach { case (value, idx) =>
          val field = fieldKeyMap(idx)
          if (value == 1) {
            val vec = embeddings.get(idx.toLong)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec.copy)
            }
          } else {
            val vec = embeddings.get(idx.toLong).mul(value)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec)
            }
          }
        }
      case s: LongFloatSparseVectorStorage =>
        s.getIndices.foreach { idx =>
          val value = s.get(idx)
          val field = fieldKeyMap(idx)
          if (value == 1) {
            val vec = embeddings.get(idx)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec.copy)
            }
          } else {
            val vec = embeddings.get(idx).mul(value)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec)
            }
          }
        }
      case s: LongFloatSortedVectorStorage =>
        s.getValues.zip(s.getIndices).foreach { case (value, idx) =>
          val field = fieldKeyMap(idx)
          if (value == 1) {
            val vec = embeddings.get(idx)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec.copy)
            }
          } else {
            val vec = embeddings.get(idx).mul(value)
            if (stats.contains(field)) {
              stats.update(field, stats(field) + 1)
              vectorMap(field).iadd(vec)
            } else {
              stats.put(field, 1)
              vectorMap.put(field, vec)
            }
          }
        }
    }

    stats.keys.toArray.sorted.map(field => vectorMap(field).idiv(stats(field))) -> stats
  }

  private def mergeUpdate(map: JMap[JLong, Vector], key: Long, update: Vector, value: Double): Unit = {
    if (!map.containsKey(key)) {
      if (value == 1) map.put(key, update.copy)
      else map.put(key, update.mul(value))
    } else {
      if (value == 1) map.get(key).iadd(update)
      else map.get(key).iadd(update.mul(value))
    }
  }

  private def getPartitions(backward: Matrix, rId: Int): Array[Vector] = {
    val vec = backward.getRow(rId)
    val method = vec.getClass.getDeclaredMethod("getPartitions")
    method.invoke(vec).asInstanceOf[Array[Vector]]
  }

  def calGradient(placeHolder: PlaceHolder, backward: Matrix, assembleHint: String,
                  assembleStats: List[mutable.HashMap[Int, Int]])(implicit graph: Graph): Matrix = {
    val features: Matrix = placeHolder.getFeats
    val gradMap: JHashMap[JLong, Vector] = new JHashMap[JLong, Vector]()

    if (assembleHint == MultiHot && placeHolder.hasFieldKeyMap) {
      val fieldKeyMap = placeHolder.getFieldKeyMap
      multiHotGradAssemble(features, backward, fieldKeyMap, assembleStats, gradMap)
    } else {
      oneHotGradAssemble(features, backward, gradMap)
    }

    val iter = gradMap.keySet().iterator()
    val normalFactor = graph.normalFactor
    while (iter.hasNext) {
      val key = iter.next()
      gradMap.get(key).imul(normalFactor)
    }

    new MapMatrix(0, 0, gradMap)
  }

  private def oneHotGradAssemble(features: Matrix, backward: Matrix,
                                 gradMap: JHashMap[JLong, Vector]): Unit = {
    (0 until features.getNumRows).foreach { rId =>
      val row = features.getRow(rId)
      val partitions = getPartitions(backward, rId)

      row.getStorage match {
        case s: IntDoubleSparseVectorStorage =>
          s.getIndices.sorted.zipWithIndex.foreach { case (key, idx) =>
            val value = s.get(key)
            val update = partitions(idx)
            mergeUpdate(gradMap, key.toLong, update, value)
          }
        case s: IntFloatSparseVectorStorage =>
          s.getIndices.sorted.zipWithIndex.foreach { case (key, idx) =>
            val value = s.get(key)
            val update = partitions(idx)
            mergeUpdate(gradMap, key.toLong, update, value)
          }
        case s: LongDoubleSparseVectorStorage =>
          s.getIndices.sorted.zipWithIndex.foreach { case (key, idx) =>
            val value = s.get(key)
            val update = partitions(idx)
            mergeUpdate(gradMap, key, update, value)
          }
        case s: LongFloatSparseVectorStorage =>
          s.getIndices.sorted.zipWithIndex.foreach { case (key, idx) =>
            val value = s.get(key)
            val update = partitions(idx)
            mergeUpdate(gradMap, key, update, value)
          }
        case _ => throw MLException("Data type is not support!")
      }
    }
  }

  private def multiHotGradAssemble(features: Matrix, backward: Matrix, fieldKeyMap: Map[Long, Int],
                                   assembleStats: List[mutable.HashMap[Int, Int]],
                                   gradMap: JHashMap[JLong, Vector]): Unit = {
    (0 until features.getNumRows).foreach { rId =>
      val row = features.getRow(rId)
      val partitions = getPartitions(backward, rId)
      val stats = assembleStats(rId)

      val sortedField = stats.keys.toArray.sorted

      row.getStorage match {
        case s: IntDoubleSparseVectorStorage =>
          s.getIndices.sorted.zipWithIndex.foreach { case (key, idx) =>
            val field = fieldKeyMap(idx.toLong)
            val value = s.get(key) / stats(field)
            val update = partitions(sortedField.indexOf(field))
            mergeUpdate(gradMap, key.toLong, update, value)
          }
        case s: IntFloatSparseVectorStorage =>
          s.getIndices.foreach { key =>
            val field = fieldKeyMap(key.toLong)
            val value = s.get(key) / stats(field)
            val update = partitions(sortedField.indexOf(field))
            mergeUpdate(gradMap, key.toLong, update, value)
          }
        case s: LongDoubleSparseVectorStorage =>
          s.getIndices.foreach { key =>
            val field = fieldKeyMap(key)
            val value = s.get(key) / stats(field)
            val update = partitions(sortedField.indexOf(field))
            mergeUpdate(gradMap, key, update, value)
          }
        case s: LongFloatSparseVectorStorage =>
          s.getIndices.foreach { key =>
            val field = fieldKeyMap(key)
            val value = s.get(key) / stats(field)
            val update = partitions(sortedField.indexOf(field))
            mergeUpdate(gradMap, key, update, value)
          }
        case _ => throw MLException("Data type is not support!")
      }
    }
  }
}
