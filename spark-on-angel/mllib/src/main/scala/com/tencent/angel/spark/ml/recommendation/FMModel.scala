package com.tencent.angel.spark.ml.recommendation

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.models.matrix.PSMatrix
import com.tencent.angel.spark.models.vector.PSVector
import breeze.linalg.{DenseVector => BZD}
import com.tencent.angel.spark.models.MLModel
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class FMModel(dim: Int, factor: Int) extends MLModel {

  var w0 = PSVector.dense(1, 1)
  var w  = PSVector.dense(dim, 1)
  val v  = PSMatrix.dense(dim, factor)


  def pullFromPS(index: Array[Int]):
    (Double, BZD[Double], mutable.HashMap[Int, BZD[Double]]) = {
    val map = new mutable.HashMap[Int, BZD[Double]]()
    v.pull(index).foreach(f => map.put(f._1, new BZD[Double](f._2)))
    (w0.pull.values(0), new BZD[Double](w.pull.values), map)
  }

  def pushToPS(w0_grad: Double,
               w_grad : BZD[Double],
               v_grad : mutable.HashMap[Int, BZD[Double]]): Unit = {

    // add w0 gradient
    val w0_grad_vector = new Array[Double](1)
    w0_grad_vector(0) = w0_grad
    PSClient.instance().denseRowOps.increment(w0, w0_grad_vector)

    // add w gradient
    PSClient.instance().denseRowOps.increment(w, w_grad.data)

    // add v gradient
    for (elem <- v_grad) {
      v.increment(elem._1, elem._2.data)
    }
  }

  override def save(modelPath: String): Unit = ???

  override def predict(input: DataFrame): DataFrame = ???
}
