package com.tencent.angel.spark.ml.util

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.network.layers.edge.inputlayer.{Embedding, SparseInputLayer}
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.matrix.{RBIntDoubleMatrix, RBIntFloatMatrix}
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage
import com.tencent.angel.ml.math2.vector.{IntFloatVector, IntIntVector, IntKeyVector}
import com.tencent.angel.ml.psf.columns.{GetColsFunc, GetColsParam, GetColsResult}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.core.GraphModel
import org.apache.spark.SparkContext

object ModelSaver {

  def save(path: String, model: GraphModel,
           denseToSparseMatrixId: Int,
           denseDim: Int): Unit = {

    println(s"saving model to path $path")
    // calculating sizes for each partition
    val numPartition = model.graph.taskNum
    val sizes = new Array[Int](numPartition)
    for (i <- 0 until numPartition) sizes(i) = denseDim / numPartition
    val left = denseDim % numPartition
    for (i <- 0 until left) sizes(i) += 1

    val offsets = new Array[(Int, Int)](numPartition)
    offsets(0) = (0, sizes(0))
    for (i <- 1 until numPartition) {
      offsets(i) = (sizes(i - 1), sizes(i) + sizes(i - 1))
      sizes(i) += sizes(i - 1)
    }

    model.graph.getTrainable.foreach { layer =>
      layer match {
        case l: SparseInputLayer =>
          save(s"$path/${l.name}", l, denseToSparseMatrixId, offsets)
        case l: Embedding =>
          save(s"$path/${l.name}", l, denseToSparseMatrixId, offsets)
      }
    }
  }

  def save(path: String,
           layer: SparseInputLayer,
           denseToSparseMatrixId: Int,
           offsets: Array[(Int, Int)]): Unit = {

    val weightId = layer.weightId

    def saveOnePartition(index: Int, iter: Iterator[(Int, Int)]): Iterator[String] = {
      val (start, end) = iter.next()
      val size = end - start
      val indices = VFactory.denseIntVector((start until end).toArray)

      // fetch denseToSparse index
      val denseToSparse = PSMatrixUtils.getRowWithIndex(denseToSparseMatrixId, 0, indices)
        .asInstanceOf[IntIntVector]

      // fetch params
      val weights = PSMatrixUtils.getRowWithIndex(weightId, 0, indices)

      val result = new Array[String](size)
      weights match {
        case v: IntFloatVector =>
          for (i <- 0 until size) {
            val value = v.get(i + start)
            val feature = denseToSparse.get(i + start)
            result(i) = s"$feature:$value"
          }
        case _ =>
          throw new AngelException("only IntFloat type is supported now !")
      }

      result.iterator
    }

    val weightsPath = path + "/" + s"${layer.name}_weights"
    SparkContext.getOrCreate().parallelize(offsets, offsets.length)
      .mapPartitionsWithIndex((index, it) => saveOnePartition(index, it))
      .saveAsTextFile(weightsPath)

    val biasPath = path + "/" + s"${layer.name}_bias"
    val bias = PSMatrixUtils.getRow(layer.biasId, 0).asInstanceOf[IntFloatVector].get(0)
    SparkContext.getOrCreate().parallelize(Array(bias), 1).saveAsTextFile(biasPath)
  }

  def save(path: String,
           layer: Embedding,
           denseToSparseMatrixId: Int,
           offsets: Array[(Int, Int)]): Unit = {
    val embeddingId = layer.matrixId
    val numFactors  = layer.numFactors

    def saveOnePartition(index: Int, iter: Iterator[(Int, Int)]): Iterator[String] = {
      val (start, end) = iter.next()
      val size = end - start
      val indices = VFactory.denseIntVector((start until end).toArray)

      // fetch denseToSparse index
      val denseToSparse = PSMatrixUtils.getRowWithIndex(denseToSparseMatrixId, 0, indices)
        .asInstanceOf[IntIntVector]

      // fetch embedding vectors
      val rows = (0 until numFactors).toArray
      val param = new GetColsParam(embeddingId, rows, indices)
      val func = new GetColsFunc(param)
      val result = PSAgentContext.get.getUserRequestAdapter.get(func).asInstanceOf[GetColsResult]
      val embeddings = result.results

      val results = new Array[String](size)
      for (i <- 0 until size) {
        val key = start + i
        val vector = embeddings.get(key.toLong)
        vector match {
          case v: IntFloatVector =>
            val values = v.getStorage.asInstanceOf[IntFloatDenseVectorStorage].getValues
            val feature = denseToSparse.get(key)
            results(i) = s"$feature ${values.mkString(",")}"
          case _ =>
            throw new AngelException("only IntFloat is supported now!")
        }
      }
      results.iterator
    }

    SparkContext.getOrCreate().parallelize(offsets, offsets.length)
      .mapPartitionsWithIndex((index, it) => saveOnePartition(index, it))
      .saveAsTextFile(path)
  }

}
