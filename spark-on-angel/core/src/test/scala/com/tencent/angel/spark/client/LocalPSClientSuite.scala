package com.tencent.angel.spark.client

import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

import scala.util.Random

class LocalPSClientSuite extends PSFunSuite with SharedPSContext {
  // PS constant parameter
  var localClient: LocalPSClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    localClient = new LocalPSClient
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("doCreateVectorPool") {
    val pool1 = localClient.createVectorPool(11, 5)
    val psKey1 = pool1.createZero()

    assert(pool1.numDimensions == 11 && pool1.capacity == 5)
    assert(psKey1.mkLocal().size == 11)

    val pool2 = localClient.createVectorPool(6, 10)
    val psKey2 = pool2.createZero()
    assert(pool2.numDimensions == 6 && pool2.capacity == 10)
    assert(psKey2.mkLocal().size == 6)

    localClient.destroyVectorPool(pool1)
    localClient.destroyVectorPool(pool2)
  }

  test("doDestroyVectorPool") {
    val pool = localClient.createVectorPool(11, 5)
    localClient.destroyVectorPool(pool)

    assert(localClient.getPool(pool.id) == null)
  }

  test("fetchArray") {
    val pool = localClient.createVectorPool(10, 5)
    val rand = new Random(42)
    val arr = (0 until 10).toArray.map(i => rand.nextGaussian())

    val psKey = pool.create(arr)

    assert(localClient.fetchArray(psKey).sameElements(arr))
    localClient.destroyVectorPool(pool)
  }

}
