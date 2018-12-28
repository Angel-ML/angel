package com.tencent.angel.spark.ml.tree.util

import com.tencent.angel.spark.ml.tree.data.{Instance, VerticalPartition => VP}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext, TaskContext}

import scala.collection.mutable.{ArrayBuilder => AB}

object DataLoader {
  def loadLibsvmDP(input: String, dim: Int)(implicit sc: SparkContext): RDD[Instance] = {
    sc.textFile(input)
      .map(_.trim)
      .filter(_.nonEmpty)
      .filter(!_.startsWith("#"))
      .map(line => parseLibsvm(line, dim))
  }

  def loadLibsvmFP(input: String, dim: Int, numPartition: Int, partitioner: Partitioner = null)
                  (implicit sc: SparkContext): RDD[VP] = {
    if (partitioner == null) {
      return loadLibsvmFP(input, dim, numPartition, new EvenPartitioner(dim, numPartition))
    }
    val bcPartitioner = sc.broadcast(partitioner)

    sc.textFile(input)
      .map(_.trim)
      .filter(_.nonEmpty)
      .filter(!_.startsWith("#"))
      .mapPartitions(iterator => {
        // initialize labels, indices and values array builders
        val labels = new AB.ofFloat
        labels.sizeHint(1 << 20)
        val indexEnd = new Array[AB.ofInt](numPartition)
        val indices = new Array[AB.ofInt](numPartition)
        val values = new Array[AB.ofFloat](numPartition)
        for (partId <- 0 until numPartition) {
          indexEnd(partId) = new AB.ofInt
          indices(partId) = new AB.ofInt
          values(partId) = new AB.ofFloat
          indexEnd(partId).sizeHint(1 << 20)
          indices(partId).sizeHint(1 << 20)
          values(partId).sizeHint(1 << 20)
        }
        // for each instance, collect its label and
        // assign its features to corresponding partition
        val curIndex = new Array[Int](numPartition)
        iterator.foreach(line => {
          val splits = line.split("\\s+|,").map(_.trim)
          labels += splits(0).toFloat
          for (i <- 0 until splits.length - 1) {
            val kv = splits(i + 1).split(":")
            val indice = kv(0).toInt
            val value = kv(1).toFloat
            val partId = bcPartitioner.value.getPartition(indice)
            indices(partId) += indice
            values(partId) += value
            curIndex(partId) += 1
          }
          for (partId <- 0 until numPartition)
            indexEnd(partId) += curIndex(partId)
        })
        // create partitions
        val labelsArr = labels.result()
        (0 until numPartition).map(partId =>
          (partId, VP(TaskContext.getPartitionId(), labelsArr,
            indexEnd(partId).result(), indices(partId).result(), values(partId).result()))
        ).iterator
      }).partitionBy(new EvenPartitioner(numPartition, numPartition))
      .mapPartitions(iterator => {
        val (partIds, partitions) = iterator.toArray.unzip
        require(partIds.distinct.length == 1)
        partitions.iterator
      })
  }

  def parseLibsvm(line: String, dim: Int): Instance = {
    val splits = line.split("\\s+|,").map(_.trim)
    val y = splits(0).toDouble

    val indices = new Array[Int](splits.length - 1)
    val values = new Array[Double](splits.length - 1)
    for (i <- 0 until splits.length - 1) {
      val kv = splits(i + 1).split(":")
      indices(i) = kv(0).toInt
      values(i) = kv(1).toDouble
    }

    Instance(y, Vectors.sparse(dim, indices, values))
  }

}
