package com.tencent.angel.spark.ml.optim

object FTRL {

  // compute the increment for z and n model for one instance
  def trainByInstance(data: (String, Array[(Long, Double)]),
                      localZ: Map[Long, Double],
                      localN: Map[Long, Double],
                      alpha: Double,
                      beta: Double,
                      lambda1: Double,
                      lambda2: Double,
                      getGradLoss:(Array[(Long, Double)], Double, Array[(Long, Double)]) => Map[Long, Double]
                     ): (Array[(Long, Double)], Array[(Long, Double)]) = {

    val label = data._1.toDouble
    val feature = data._2
    // the number of not zero of feature
    val featLeg = feature.length

    // init w which is the weight of the model
    val localW = new Array[(Long, Double)](featLeg)

    // update the w
    (0 until featLeg).foreach{ i =>
      val fId = feature(i)._1
      val zVal = localZ.getOrElse(fId, 0.0)
      val nVal = localN.getOrElse(fId, 0.0)
      // w_local的更新
      localW(i) = (fId, updateWeight(zVal, nVal, alpha, beta, lambda1, lambda2))
    }

    var gOnId = 0.0
    var dOnId = 0.0

    // update z and n in all dimension
    val incrementZ = new Array[(Long, Double)](featLeg)
    val incrementN = new Array[(Long, Double)](featLeg)

    (0 until featLeg).foreach{ i =>

      val fId = feature(i)._1
      val nVal = localN.getOrElse(fId, 0.0)

      // G(t):第t次迭代中损失函数梯度，g(t)表示某一维度上的梯度
      gOnId = getGradLoss(localW, label, feature).getOrElse(fId, 0.0)
      // delta(s),n_val初始为0，z(i)初始为0
      dOnId = 1.0 / alpha * (Math.sqrt(nVal + gOnId * gOnId) - Math.sqrt(nVal))

      incrementZ(i) = (fId, gOnId - dOnId * localW(i)._2)
      incrementN(i) = (fId, gOnId * gOnId)
    }

    (incrementZ, incrementN)
  }

  // compute new weight
  def updateWeight(zOnId: Double,
                   nOnId: Double,
                   alpha: Double,
                   beta: Double,
                   lambda1: Double,
                   lambda2: Double): Double = {
    if (Math.abs(zOnId) <= lambda1)
      0.0
    else
      (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nOnId)) / alpha)) * (zOnId - Math.signum(zOnId).toInt * lambda1)
  }

}