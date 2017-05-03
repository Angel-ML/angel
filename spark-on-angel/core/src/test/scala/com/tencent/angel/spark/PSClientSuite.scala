package com.tencent.angel.spark

import com.tencent.angel.spark.client.{AngelPSClient, LocalPSClient}

class PSClientSuite extends PSFunSuite with SharedPSContext {

  // PS constant parameter
  val dim = 10
  val capacity = 10
  var _psClient: PSClient = _
  var _pool: PSVectorPool = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _psClient = PSClient.get
    _pool = _psClient.createVectorPool(dim, capacity)
  }

  override def afterAll(): Unit = {
    _psClient.destroyVectorPool(_pool)
    super.afterAll()
  }

  test("setup PSClient") {
    PSClient.setup(sc)
    val psClient = PSClient.get

    Thread.sleep(100)

    assert(psClient.isInstanceOf[LocalPSClient] || psClient.isInstanceOf[AngelPSClient])

    PSClient.cleanup(sc)
  }

  test("cleanup PSClient") {
    PSClient.setup(sc)

    PSClient.cleanup(sc)
    if (!sc.isLocal) {
       assert(!AngelPSClient.isAlive)
    }
  }

  test("create vector pool") {
    val psClient = PSClient.get
    val pool = psClient.createVectorPool(dim, capacity)

    val proxy = pool.createZero()

    assert(pool.numDimensions == dim)
    assert(pool.capacity == capacity)
    assert(proxy.mkLocal().size == dim)
    assert(proxy.mkLocal().get().sameElements(Array.ofDim[Double](dim)))

    psClient.destroyVectorPool(pool)
  }

  test("destroy vector pool") {
    val psClient = PSClient.get
    val pool = psClient.createVectorPool(dim, capacity)

    psClient.destroyVectorPool(pool)
    assert(psClient.getPool(pool.id) == null)
  }

}
