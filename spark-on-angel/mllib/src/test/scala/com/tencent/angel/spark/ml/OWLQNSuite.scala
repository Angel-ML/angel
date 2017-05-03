package com.tencent.angel.spark.ml

import breeze.linalg.norm
import breeze.optimize.DiffFunction
import com.tencent.angel.spark.PSClient
import com.tencent.angel.spark.ml.optim.OWLQN
import com.tencent.angel.spark.vector.BreezePSVector

class OWLQNSuite extends PSFunSuite with SharedPSContext {

  test("super simple") {
    val dim = 3
    val capacity = 20

    val pool = PSClient.get.createVectorPool(dim, capacity)

    val l1reg = pool.create(1.0).mkBreeze()

    val owlqn = new OWLQN(100, 4, l1reg)

    def optimizeThis(init: BreezePSVector) = {
      val f = new DiffFunction[BreezePSVector] {
        def calculate(x: BreezePSVector) = {
          (norm((x - 3.0) :^ 2.0, 1), (x :* 2.0) - 6.0)
        }
      }

      val result = owlqn.minimize(f, init)
      result
    }

    val initWeightPS = pool.create(Array(-1.1053, 0.0, 0.0)).mkBreeze()
    val result = optimizeThis(initWeightPS)
    assert((result.toLocal.get()(0) - 2.5) < 1E-4, result)
  }


  test("optimize a simple multivariate gaussian") {
    val dim = 3
    val capacity = 20

    val pool = PSClient.get.createVectorPool(dim, capacity)
    val l1reg = pool.create(1.0).mkBreeze()
    val lbfgs = new OWLQN(100, 4, l1reg)

    def optimizeThis(init: BreezePSVector) = {
      val f = new DiffFunction[BreezePSVector] {
        def calculate(x: BreezePSVector) = {
          (math.pow(norm(x - 3.0, 2), 2), (x :* 2.0) - 6.0)
        }
      }

      lbfgs.minimize(f, init)
    }

    val initWeightPS = pool.create(Array(0.0, 0.0, 0.0)).mkBreeze()
    val result = optimizeThis(initWeightPS)

    assert(norm(result - 2.5, 2) < 1E-4)
  }

}
