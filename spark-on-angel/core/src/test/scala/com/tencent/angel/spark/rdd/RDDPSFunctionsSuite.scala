package com.tencent.angel.spark.rdd

import com.tencent.angel.spark.{PSClient, PSFunSuite, SharedPSContext}


class RDDPSFunctionsSuite extends PSFunSuite with SharedPSContext {

  test("psAggregate") {
    import RDDPSFunctions._
    val seed = 0 until 100
    val dim = 10
    val capacity = 10
    val rdd = sc.parallelize(seed, 1).map { i =>
      Array.fill[Int](dim)(i)
    }

    val psClient = PSClient.get
    val pool = psClient.createVectorPool(dim, capacity)
    val remoteVector = pool.createZero().mkRemote()

    def seqOp: (Int, Array[Int]) => Int = { (c: Int, x: Array[Int]) =>
      remoteVector.increment(x.map(_.toDouble))
      c + 1
    }
    def combOp: (Int, Int) => Int = (c1: Int, c2: Int) => c1 + c2
    val count = rdd.psAggregate(0)(seqOp, combOp)

    val result = Array.fill[Int](dim)(seed.sum)
    assert(count === seed.length)
    assert(remoteVector.toLocal.get().map(_.toInt).sameElements(result))

    pool.delete(remoteVector.proxy)
    psClient.destroyVectorPool(pool)
  }

  test("psFoldLeft") {
    import RDDPSFunctions._

    val seed = 0 until 100
    val dim = 10
    val capacity = 10
    val rdd = sc.parallelize(seed, 1).map { i =>
      Array.fill[Int](dim)(i)
    }

    val psClient = PSClient.get
    val pool = psClient.createVectorPool(dim, capacity)

    val remoteVector = pool.create(Double.NegativeInfinity).mkRemote()

    val max = rdd.psFoldLeft(remoteVector) { (pv, bv) =>
      pv.mergeMax(bv.map(_.toDouble))
      pv
    }

    assert(max.toLocal.get().map(_.toInt).sameElements(Array.fill(dim)(99)))

    pool.delete(remoteVector.proxy)
    psClient.destroyVectorPool(pool)
  }
}
