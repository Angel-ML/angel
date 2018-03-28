package com.tencent.angel.spark.ml.recommendation

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.common.Instance
import com.tencent.angel.spark.ml.util.DataLoader
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.{SparkConf, SparkContext}

object FMRunner {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc   = new SparkContext(conf)
    val path = args(0)
    val data = DataLoader.loadLibsvm(path, 10, 1.0, -1)
    val feat = data.take(1)(0)._2.asInstanceOf[SparseVector].size

    println(s"dimension=$feat")


    PSContext.getOrCreate(sc)

    val fm = new FMLearner
    fm.setFactor(10)
    fm.setLearningRate(0.5)
    fm.setDim(feat)


    val samples = data.map(f => new Instance(f._1, f._2))
    fm.train(samples)

  }

}
