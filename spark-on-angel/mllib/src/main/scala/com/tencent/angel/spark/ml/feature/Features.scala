package com.tencent.angel.spark.ml.feature

import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.LongIntVector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.context.PSContext
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import scala.util.hashing.MurmurHash3

/**
  * This class provide some feature pre-processing utils for input data.
  */
object Features {

  def libSVMStringToIndex(data: RDD[String]): (RDD[LabeledData], Map[String, Int]) = {
    null
  }

  /**
    * Convert a corpus dataset (with strings) to dataset with IDs.
    * @param data: corpus data with strings
    * @return
    */
  def corpusStringToInt(data: RDD[String]): (RDD[Array[Int]], Array[(Int, String)]) = {
    // cache data
    data.cache().count()


    // All distinct strings
    val features = data.filter(f => f != null && f.length > 0)
        .map(f => f.stripLineEnd.split(" ")).flatMap(f => f)
        .map(t => (t, 1)).reduceByKey(_ + _).map(f => f._1)

    // Mapping each string with a hashcode ID, the hashcode is sparse
    // Now we use the default hashcode of string in Java.
    val featureWithIndex = features.map(f => (f, MurmurHash3.stringHash(f)))
    featureWithIndex.cache().count()

    println(s"feature count ${featureWithIndex.map(f => f._1).distinct().count()}")
    println(s"hashcode count ${featureWithIndex.map(f => f._2).distinct().count()}")

    // Now convert the sparse index to dense index, we use
    val sparseDim: Long = featureWithIndex.map(f => f._2).max().toLong + 1
    val denseIndices = featureWithIndex.map(f => f._2).zipWithIndex()
    denseIndices.cache().count()
    // The dense dimension
    val denseDim = (denseIndices.map(f => f._2).max() + 1).toInt

    // Create an index matrix on servers to mapping from sparse index to dense index
    val sparseToDense = PSMatrixUtils.createPSMatrixCtx("sparseToDense", 1, sparseDim, RowType.T_INT_SPARSE_LONGKEY)
    val sparseToDenseMatrixId = PSMatrixUtils.createPSMatrix(sparseToDense)

    val denseToSparse = PSMatrixUtils.createPSMatrixCtx("denseToSparse", 1, denseDim, RowType.T_LONG_DENSE)
    val denseToSparseMatrixId = PSMatrixUtils.createPSMatrix(denseToSparse)

    // Function to initialize sparseToDenseMatrix and denseToSparseMatrix
    def initializeSparseAndDenseMatrix(iterator: Iterator[(Int, Long)],
                                       sparseDim: Long,
                                       sparseToDenseMatrixId: Int,
                                       denseDim: Int,
                                       denseToSparseMatrixId: Int): Iterator[Int] = {
      PSContext.instance()

      // Initialize sparseToDense and denseToDense matrix
      val sparseToDenseUpdate = VFactory.sparseLongKeyIntVector(sparseDim)
      val denseToSparseUpdate = VFactory.sparseLongVector(denseDim)
      while (iterator.hasNext) {
        val (sparseIndex, denseIndex) = iterator.next()
        sparseToDenseUpdate.set(sparseIndex, denseIndex.toInt)
        denseToSparseUpdate.set(denseIndex.toInt, sparseIndex)
      }
      // Push updates to servers
      PSMatrixUtils.incrementRow(sparseToDenseMatrixId, 0, sparseToDenseUpdate)
      PSMatrixUtils.incrementRow(denseToSparseMatrixId, 0, denseToSparseUpdate)

      Iterator.single(0)
    }

    // Function to convert corpus data from strings to int IDs.
    def doSparseToDense(iterator: Iterator[String],
                        sparseToDenseMatrixId: Int): Iterator[Array[Int]] = {
      PSContext.instance()
      val set = new LongOpenHashSet()
      val corpus = new ArrayBuffer[Array[Int]]()

      while (iterator.hasNext) {
        val line = iterator.next()
        if (line != null && line.length > 0) {
          val strings = line.stripLineEnd.split(" ")
          val ints = strings.map(s => MurmurHash3.stringHash(s))
          corpus.append(ints)
          ints.foreach(index => set.add(index))
        }
      }

      val pullIndex = VFactory.denseLongVector(set.toLongArray())
      val sparseToDenseIndex = PSMatrixUtils.getRowWithIndex(sparseToDenseMatrixId, 0, pullIndex)
        .asInstanceOf[LongIntVector]

      val size = corpus.size
      var i = 0
      while (i < size) {
        val hashcodes = corpus(i)
        var j = 0
        while (j < hashcodes.length) {
          hashcodes(j) = sparseToDenseIndex.get(hashcodes(j))
          j += 1
        }
        i += 1
      }

      corpus.iterator
    }

    def buildDenseToString(iterator: Iterator[(String, Int)],
                           sparseDim: Long,
                           sparseToDenseMatrixId: Int): Iterator[(Int, String)] = {
      val stringsWithInts = iterator.toArray
      val indices = new Array[Long](stringsWithInts.length)
      var i = 0
      while (i < indices.length) {
        indices(i) = stringsWithInts(i)._2
        i += 1
      }

      val sparseToDenseIndex = PSMatrixUtils.getRowWithIndex(sparseToDenseMatrixId, 0, VFactory.denseLongVector(indices))
          .asInstanceOf[LongIntVector]
      stringsWithInts.map(f => (sparseToDenseIndex.get(f._2), f._1)).iterator
    }

    // Initialize two index matrix
    denseIndices.mapPartitions(it =>
      initializeSparseAndDenseMatrix(it,
        sparseDim,
        sparseToDenseMatrixId,
        denseDim,
        denseToSparseMatrixId),
      true).count()

    denseIndices.unpersist()

    // Converting strings to ints
    val intCorpus = data.mapPartitions(it =>
      doSparseToDense(it, sparseToDenseMatrixId),
      true)

    intCorpus.cache().count()

    // Mapping index
    val denseToString = featureWithIndex.mapPartitions(it =>
      buildDenseToString(it,
        sparseDim,
        sparseToDenseMatrixId))
        .collect()

    data.unpersist()
    featureWithIndex.unpersist()

    (intCorpus, denseToString)
  }

}
