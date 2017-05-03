package com.tencent.angel.spark.examples

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.examples.util.Logistic
import com.tencent.angel.spark.examples.util.PSExamples._
import com.tencent.angel.spark.PSClient
import com.tencent.angel.spark.PSVectorProxy
import com.tencent.angel.spark.rdd.RDDPSFunctions._
import com.tencent.angel.spark.vector.RemotePSVector

/**
 * These are examples of RDDFunction.psAggregate and RDDFunction.foldLeft
 */
object VectorAggregation {

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runWithSparkContext(this.getClass.getSimpleName) { sc =>
      PSClient.setup(sc)
      val vectorRDD = Logistic.generateLRData(N, DIM, numSlices)
        .map (x => new DenseVector[Double](x._1.toArray))

      run(vectorRDD)
      runWithPS(vectorRDD, DIM)
    }
  }

  private def run(data: RDD[DenseVector[Double]]): Unit = {
    println("sum" + data.reduce(_ + _))
    println("max" + data.reduce(breeze.linalg.max(_, _)))
    println("min" + data.reduce(breeze.linalg.min(_, _)))
  }

  private def runWithPS(data: RDD[DenseVector[Double]], dim: Int): Unit = {

    val psClient = PSClient.get
    val pool = psClient.createVectorPool(dim, 2)

    var vecKey: PSVectorProxy = null
    var vec: RemotePSVector = null
    var result: RemotePSVector = null

    vecKey = pool.createZero()
    vec = vecKey.mkRemote()
    result = data.psFoldLeft(vec) { (pv, bv) =>
      pv.increment(bv.toArray)
      pv
    }
    println("sum" + psClient.get(result.proxy).mkString(", "))
    vecKey.delete()

    vecKey = pool.create(Double.NegativeInfinity)
    vec = vecKey.mkRemote()
    result = data.psFoldLeft(vec) { (pv, bv) =>
      pv.mergeMax(bv.toArray)
      pv
    }
    println("max" + psClient.get(result.proxy).mkString(", "))
    vecKey.delete()

    vecKey = pool.create(Double.PositiveInfinity)
    vec = vecKey.mkRemote()
    result = data.psFoldLeft(vec) { (pv, bv) =>
      pv.mergeMin(bv.toArray)
      pv
    }
    println("min" + psClient.get(result.proxy).mkString(", "))
    vecKey.delete()

    psClient.destroyVectorPool(pool)
  }

}
