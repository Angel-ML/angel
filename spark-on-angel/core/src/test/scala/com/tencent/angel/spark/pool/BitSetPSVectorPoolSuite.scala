package com.tencent.angel.spark.pool

import com.tencent.angel.spark.{PSClient, PSFunSuite, PSVectorProxy, SharedPSContext}

class BitSetPSVectorPoolSuite extends PSFunSuite with SharedPSContext {

  test("allocate") {
    val dim = 10
    val capacity = 10
    val pool = PSClient.get.createVectorPool(dim, capacity).asInstanceOf[BitSetPSVectorPool]

    var proxys: Array[PSVectorProxy] = null

    try {
      proxys = (0 until capacity).toArray.map { _ =>
        pool.allocate()
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        assert(true)
    }

    val releaseNum = 5
    proxys.slice(0, releaseNum).foreach { key =>
      pool.delete(key)
    }

    try {
      (0 until capacity - releaseNum).foreach { i =>
        pool.allocate()
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        assert(true)
    }

    proxys.slice(releaseNum, capacity).foreach { key =>
      pool.delete(key)
    }
  }
}
