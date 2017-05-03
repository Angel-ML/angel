package com.tencent.angel.spark.rdd

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.PSClient

class RDDPSFunctions[T: ClassTag](self: RDD[T]) extends Serializable {

  /**
   * psAggregate is similar to RDD.aggregate, you can update the PSVector in `seqOp` at the same
   * time of aggregation.
   *
   * @param zeroValue the initial value
   * @param seqOp an operator used to accumulate results within a partition
   * @param combOp an associative operator used to combine results from different partitions
   */
  def psAggregate[U: ClassTag](zeroValue: U)(
      seqOp: (U, T) => U,
      combOp: (U, U) => U): U = {
    self.mapPartitions { iter =>
      val result = iter.foldLeft(zeroValue)(seqOp)
      PSClient.get.flush()
      Iterator(result)
    }.reduce(combOp)
  }

  def psFoldLeft[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U): U = {
    self.mapPartitions { iter =>
      val result = iter.foldLeft(zeroValue)(seqOp)
      PSClient.get.flush()
      Iterator(result)
    }.collect().head
  }

}

object RDDPSFunctions {

  /** Implicit conversion from an RDD to RDDFunctions. */
  implicit def fromRDD[T: ClassTag](rdd: RDD[T]): RDDPSFunctions[T] = {
    new RDDPSFunctions[T](rdd)
  }

}
