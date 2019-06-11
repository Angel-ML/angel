package com.tencent.angel.spark.ml.graph.utils

import java.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, TaskContext}

import scala.reflect.ClassTag
import scala.util.{Random => ScalaRandom}

class PartitionwiseWeightedSampledRDDPartition(val prev: Partition, val seed: Long, val fraction: Double)
  extends Partition with Serializable {
  override val index: Int = prev.index
}

/**
  * An RDD sampled from its parent RDD partition-wise with weights. For each partition of the parent RDD,
  * a user-specified [[WeightedRandomSampler]] instance is used to obtain
  * a random sample of the records in the partition. The random seeds assigned to the samplers
  * are guaranteed to have different values.
  *
  * @param prev                  RDD to be sampled
  * @param sampler               a random weighted sampler
  * @param preservesPartitioning whether the sampler preserves the partitioner of the parent RDD
  * @param seed                  random seed
  * @tparam T input RDD key type
  * @tparam U sampled RDD item type
  */
class PartitionwiseWeightedSampledRDD[T: ClassTag, U: ClassTag](
                                                                 prev: RDD[(T, Float)],
                                                                 sampler: WeightedRandomSampler[T, U],
                                                                 fractions: Map[Int, Double],
                                                                 preservesPartitioning: Boolean,
                                                                 @transient private val seed: Long = ScalaRandom.nextLong)
  extends RDD[U](prev) {

  @transient override val partitioner: Option[Partitioner] = {
    if (preservesPartitioning) prev.partitioner else None
  }

  override def getPartitions: Array[Partition] = {
    val random = new Random(seed)
    firstParent[(T, Float)].partitions.map { x =>
      new PartitionwiseWeightedSampledRDDPartition(x, random.nextLong(), fractions.getOrElse(x.index, 0.0))
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    firstParent[(T, Float)].preferredLocations(
      split.asInstanceOf[PartitionwiseWeightedSampledRDDPartition].prev
    )
  }

  override def compute(splitIn: Partition, context: TaskContext): Iterator[U] = {
    val split = splitIn.asInstanceOf[PartitionwiseWeightedSampledRDDPartition]
    val thisSampler = sampler.clone
    thisSampler.setSeed(split.seed)
    thisSampler.setFraction(split.fraction)
    thisSampler.sample(firstParent[(T, Float)].iterator(split.prev, context))
  }
}