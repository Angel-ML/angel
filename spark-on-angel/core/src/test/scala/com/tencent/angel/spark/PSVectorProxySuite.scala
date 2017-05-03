package com.tencent.angel.spark

class PSVectorProxySuite extends PSFunSuite with SharedPSContext {

  private val dim = 10
  private val capacity = 10
  private var _psClient: PSClient = _
  private var _pool: PSVectorPool = _
  private var _psVectorProxy: PSVectorProxy = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _psClient = PSClient.get
    _pool = _psClient.createVectorPool(dim, capacity)
    _psVectorProxy = _pool.createZero()
  }

  override def afterAll(): Unit = {
    _pool.delete(_psVectorProxy)
    _psClient.destroyVectorPool(_pool)
    super.afterAll()
  }

  test("getPool") {
    val pool = _psVectorProxy.getPool()

    assert(pool.id == _pool.id)
  }

  test("mkLocal") {
    val localVector = _psVectorProxy.mkLocal()

    assert(_psClient.get(_psVectorProxy).sameElements(localVector.get()))
  }

  test("mkRemote") {
    val remoteVector = _psVectorProxy.mkRemote()

    assert(_psClient.get(_psVectorProxy).sameElements(remoteVector.proxy.mkLocal().get()))
  }

  test("mkBreeze") {
    val brzVector = _psVectorProxy.mkBreeze()

    assert(_psClient.get(_psVectorProxy).sameElements(brzVector.proxy.mkLocal().get()))
  }

  test("delete") {
    val psKey = _pool.createZero()
    psKey.assertValid()

    psKey.delete()
  }
}
