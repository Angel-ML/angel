package com.tencent.angel.spark.vector

import com.tencent.angel.spark.{PSClient, PSFunSuite, SharedPSContext}


class LocalPSVectorSuite extends PSFunSuite with SharedPSContext {

  test("get") {
    val dim = 10
    val capacity = 10
    val psClient = PSClient.get
    val pool = psClient.createVectorPool(dim, capacity)

    val localVector = pool.createRandomNormal(0.0, 1.0).mkLocal()
    val psKey = localVector.proxy

    assert(localVector.get().sameElements(psClient.get(psKey)))
  }
}
