package com.tencent.angel.spark.examples.ml

import org.apache.spark.ml.classification.ps.{LogisticRegression => PSLR}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.PSClient
import com.tencent.angel.spark.examples.util.Logistic
import com.tencent.angel.spark.examples.util.PSExamples._

object LogisticRegression {

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runWithSparkContext(this.getClass.getSimpleName) { sc =>
      PSClient.setup(sc)

      run(N, DIM, numSlices, ITERATIONS)
    }
  }

  case class Instance(label: Double, weight: Double, feature: Vector)

  def run(sampleNum: Int, featNum: Int, partitionNum: Int, maxIter: Int, regParam: Double = 0.01) {
    val tol = 1e-6
    val elasticNet = 0.0

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val trainSet = Logistic.generateLRData(sampleNum, featNum, partitionNum)
      .map { case (feat, label) => Instance(label, 1.0, feat)}
      .toDS()

    val lor = new PSLR()
      .setFeaturesCol("feature")
      .setWeightCol("weight")
      .setLabelCol("label")
      .setRegParam(regParam)
      .setElasticNetParam(elasticNet)
      .setMaxIter(maxIter)
      .setTol(tol)

    val lorModel = lor.fit(trainSet)
    println(s"model size: ${lorModel.coefficientMatrix.toArray.length}")
    println(s"trained model: ${lorModel.coefficientMatrix.toArray.mkString(" ")}")
    println(s"intercept: ${lorModel.interceptVector.toArray.mkString(" ")}")
  }
}

// scalastyle:on println
