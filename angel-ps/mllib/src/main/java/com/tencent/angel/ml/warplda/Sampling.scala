package com.tencent.angel.ml.warplda

import java.util.Random

/**
  * Created by takun on 01/08/2017.
  */
trait Sampling extends Serializable{
  type T
  private var seed = System.currentTimeMillis()
  val r = new Random(seed)
  def apply():T
  def setSeed(seed:Long):this.type = {
    this.seed = seed
    this
  }
  def getSeed = seed
}

trait DoubleSampling extends Sampling {
  type T = Double
}

trait IntSampling extends Sampling {
  type T = Int
}