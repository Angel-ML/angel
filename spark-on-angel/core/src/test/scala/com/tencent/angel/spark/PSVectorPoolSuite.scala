package com.tencent.angel.spark

import scala.util.Random


class PSVectorPoolSuite extends PSFunSuite with SharedPSContext {

  val dim = 10
  val capacity = 5
  private var _psClient: PSClient = _
  private var _pool: PSVectorPool = _
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    _psClient = PSClient.get
    _pool = _psClient.createVectorPool(dim, capacity)
  }

  override def afterAll(): Unit = {
    _psClient.destroyVectorPool(_pool)
    super.afterAll()
  }
  
  test("create") {
    val rand = new Random(42)
    val array = (0 until dim).toArray.map(i => rand.nextGaussian())
    val proxy = _pool.create(array)
    
    assert(proxy.mkLocal().get().sameElements(array))
  }

  test("fill") {
    val proxy = _pool.create(3.1415)
    assert(proxy.mkLocal().get().sameElements(Array.fill(dim)(3.1415)))
  }

  test("randomUniform") {
    val proxy = _pool.createRandomUniform(0.0, 1.0)

    var isCorrect = true
    proxy.mkLocal().get().foreach(x => if (x < 0.0 || x > 1.0) isCorrect = false )
    assert(isCorrect)
  }

  test("randomNormal") {
    val pool = _psClient.createVectorPool(10000, 2)
    val proxy = pool.createRandomNormal(0.0, 1.0)

    val array = proxy.mkLocal().get()
    val mean = array.sum / array.length
    val variety = array.map(x => math.pow(x - mean, 2.0)).sum / (array.length - 1)

    val tol = 0.1
    assert(math.abs(mean - 0.0) < tol)
    assert(math.abs(math.sqrt(variety) - 1.0) < tol)
  }

}
