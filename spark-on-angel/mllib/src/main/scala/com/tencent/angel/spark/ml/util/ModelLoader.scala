package com.tencent.angel.spark.ml.util

import java.lang.{Long => JLong}
import java.util.{HashMap => JHashMap, Map => JMap}

import com.tencent.angel.ml.core.network.layers.edge.inputlayer.{Embedding, SparseInputLayer}
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntIntVector, Vector}
import com.tencent.angel.ml.psf.columns._
import com.tencent.angel.ps.server.data.request.UpdateOp
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.core.GraphModel
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntOpenHashSet}
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

object ModelLoader {

  def load(path: String,
           model: GraphModel,
           sparseToDenseMatrixId: Int,
           denseDim: Int): Unit = {

    model.graph.getTrainable.foreach { layer =>
      layer match {
        case l: SparseInputLayer =>
          load(s"$path/${l.name}", l, sparseToDenseMatrixId, denseDim)
        case l: Embedding =>
          load(s"$path/${l.name}", l, sparseToDenseMatrixId)
      }
    }
  }

  def load(path: String,
           layer: SparseInputLayer,
           sparseToDenseMatrixId: Int,
           denseDim: Int): Unit = {

    println(s"loading layer ${layer.name} from path $path")
    val weightId = layer.weightId
    val biasId = layer.biasId

    def loadOnePartition(index: Int, iter: Iterator[String]): Iterator[Int] = {
      val keyValues = new ArrayBuffer[(Int, Float)]
      val set = new IntOpenHashSet()
      while (iter.hasNext) {
        val parts = iter.next().split(":")
        val (feature, value) = (parts(0).toInt, parts(1).toFloat)
        keyValues.append((feature, value))
        set.add(feature)
      }

      val indices = VFactory.denseIntVector(set.toIntArray())
      // fetch sparseToDense index
      val sparseToDense = PSMatrixUtils.getRowWithIndex(1, sparseToDenseMatrixId,
        0, indices).asInstanceOf[IntIntVector]

      val update = VFactory.sparseFloatVector(denseDim)
      keyValues.foreach(f =>
        update.set(sparseToDense.get(f._1), f._2))
      PSMatrixUtils.updateRow(weightId, 0, update)
      Iterator.single(0)
    }

    // update weights
    val weightsPath = path + "/" + s"${layer.name}_weights"
    SparkContext.getOrCreate().textFile(weightsPath)
      .mapPartitionsWithIndex((index, it) => loadOnePartition(index, it))
      .count()

    // update bias
    val biasPath = path + "/" + s"${layer.name}_bias"
    val bias = SparkContext.getOrCreate().textFile(biasPath).map(f => f.toFloat).collect()
    val biasUpdate = VFactory.denseFloatVector(bias)
    PSMatrixUtils.updateRow(biasId, 0, biasUpdate)
  }

  def load(path: String,
           layer: Embedding,
           sparseToDenseMatrixId: Int): Unit = {
    println(s"loading layer ${layer.name} from path $path")

    val embeddingId = layer.matrixId
    val numFactors  = layer.numFactors

    def loadOnePartition(index: Int, iter: Iterator[String]): Iterator[Int] = {
      val map: JMap[JLong, Vector] = new JHashMap()
      val features = new IntArrayList()

      while (iter.hasNext) {
        val line = iter.next()
        val parts = line.stripLineEnd.split(" ")
        val (feature, factors) = (parts(0).toInt, parts(1).split(",").map(f => f.toFloat))
        map.put(feature.toLong, VFactory.denseFloatVector(factors))
        features.add(feature)
      }

      // fetch sparseToDense vectors
      val indices = VFactory.denseIntVector(features.toIntArray())
      val sparseToDense = PSMatrixUtils.getRowWithIndex(1, sparseToDenseMatrixId, 0, indices)
        .asInstanceOf[IntIntVector]

      // change feature index
      val update: JMap[JLong, Vector] = new JHashMap()
      val embeddingIndices = new IntArrayList()
      val it = map.keySet().iterator()
      while (it.hasNext) {
        val feature = it.next()
        val key = sparseToDense.get(feature.toInt)
        embeddingIndices.add(key)
        update.put(key.toLong, map.get(feature))
      }

      // update embedding
      val rows = (0 until numFactors).toArray
      val updateIndicesValues = embeddingIndices.toIntArray()
      Sorting.quickSort(updateIndicesValues)
      val updateIndices = VFactory.denseIntVector(updateIndicesValues)
      val param = new UpdateColsParam(embeddingId, rows, updateIndices, update, UpdateOp.REPLACE)
      val func  = new UpdateColsFunc(param)
      PSAgentContext.get().getUserRequestAdapter.update(func).get()

      Iterator.single(0)
    }

    SparkContext.getOrCreate().textFile(path)
      .mapPartitionsWithIndex((index, it) => loadOnePartition(index, it))
      .count()
  }
}
