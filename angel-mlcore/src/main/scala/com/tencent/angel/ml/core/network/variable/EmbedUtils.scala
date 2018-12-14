package com.tencent.angel.ml.core.network.variable

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.vector._

import java.lang.{Long => JLong}
import java.util.{Map => JMap}

object EmbedUtils {
  def geneMatrix(graph: Graph, embeddings: JMap[JLong, Vector]): Matrix = {
    val features: Matrix = graph.placeHolder.getFeats
    val rows = (0 until features.getNumRows).toArray.map { rId =>
      val row = features.getRow(rId)
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

      partitions.head match {
        case v: IntDoubleVector =>
          VFactory.compIntDoubleVector(
            v.size() * partitions.length, partitions.map(_.asInstanceOf[IntDoubleVector]), v.size()
          )
        case v: IntFloatVector =>
          VFactory.compIntFloatVector(
            v.size() * partitions.length, partitions.map(_.asInstanceOf[IntFloatVector]), v.size()
          )
        case v: LongDoubleVector =>
          VFactory.compLongDoubleVector(
            v.size() * partitions.length, partitions.map(_.asInstanceOf[LongDoubleVector]), v.size()
          )
        case v: LongFloatVector =>
          VFactory.compLongFloatVector(
            v.size() * partitions.length, partitions.map(_.asInstanceOf[LongFloatVector]), v.size()
          )
      }
    }

    rows.head match {
      case _: CompIntDoubleVector =>
        MFactory.rbCompIntDoubleMatrix(rows.map(_.asInstanceOf[CompIntDoubleVector]))
      case _: CompIntFloatVector =>
        MFactory.rbCompIntFloatMatrix(rows.map(_.asInstanceOf[CompIntFloatVector]))
      case _: CompLongDoubleVector =>
        MFactory.rbCompLongDoubleMatrix(rows.map(_.asInstanceOf[CompLongDoubleVector]))
      case _: CompLongFloatVector =>
        MFactory.rbCompLongFloatMatrix(rows.map(_.asInstanceOf[CompLongFloatVector]))
    }
  }
}
