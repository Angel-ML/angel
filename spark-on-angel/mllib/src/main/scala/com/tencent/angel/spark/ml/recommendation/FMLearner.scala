package com.tencent.angel.spark.ml.recommendation

import breeze.linalg.{DenseVector => BZD, SparseVector => BZS}
import com.tencent.angel.spark.ml.common.{Instance, Learner}
import com.tencent.angel.spark.models.MLModel
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class FMLearner extends Serializable with Learner {

  var reg0: Double = 0.0
  var regw: Double = 0.0
  var regv: Double = 0.0

  var factor: Int = 0
  var dim: Int = 0
  var learn_rate: Double = 0.01


  def setReg0(reg0: Double): Unit = {
    this.reg0 = reg0
  }

  def setRegw(regw: Double): Unit = {
    this.regw = regw
  }

  def setRegv(regv: Double): Unit = {
    this.regv = regv
  }

  def setFactor(factor: Int): Unit = {
    this.factor = factor
  }

  def setLearningRate(learningRate: Double): Unit = {
    this.learn_rate = learningRate
  }

  def setDim(dim: Int): Unit = {
    this.dim = dim
  }

  def predictInstance(feature: BZS[Double],
                      w0: Double,
                      w: BZD[Double],
                      v: mutable.HashMap[Int, BZD[Double]],
                      sum_f: Array[Double]): Double = {
    var ret = 0.0
    ret += w0
    ret += feature.t * w

    val indices = feature.index
    val values  = feature.data
    for (f <- 0 until factor) {
      var sum = 0.0
      var sum_sqrt = 0.0

      for (i <- 0 until feature.activeSize) {
        val tmp = v(indices(i))(f) * values(i)
        sum += tmp
        sum_sqrt += tmp * tmp
      }

      sum_f(f) = sum
      ret += 0.5 * (sum * sum - sum_sqrt)
    }

    ret
  }

  def predict(data: RDD[(BZS[Double], Int)], fm: FMModel): RDD[(Int, Double)] = {

    def predictOnePartition(iter: Iterator[(BZS[Double], Int)]):
      Iterator[(Int, Double)] = {
      // build feature index
      val featIdx = new IntOpenHashSet()
      val buf = new mutable.ArrayBuffer[((BZS[Double], Int))]()
      while (iter.hasNext) {
        val (feature, label) = iter.next()
        for (elem <- feature.index) {featIdx.add(elem)}
        buf.append((feature, label))
      }

      val (w0, w, v) = fm.pullFromPS(featIdx.toIntArray())

      val pred = new ArrayBuffer[(Int, Double)](buf.size)
      val temp = new Array[Double](factor)
      buf.foreach { case (feat, label) =>
        val prod = predictInstance(feat, w0, w, v, temp)
        val score = 1.0 / (1.0 + Math.exp(-prod))
        pred.append((label, score))
      }
      pred.iterator
    }

    data.mapPartitions(predictOnePartition)
  }

  def evaluate(score: RDD[(Int, Double)]): (Double, Double) = {
    val sorted = score.sortBy(f => f._2)
    sorted.cache()
    val numTotal = sorted.count()
    val numPositive = sorted.map(f => f._1.toLong).reduce(_ + _)
    val numNegetive = numTotal - numPositive
    val sumRanks = sorted.zipWithIndex().filter(f => f._1._1 == 1).map(f => f._2 + 1).reduce(_ + _)
    val auc = (sumRanks - (numPositive * (numPositive + 1.0)) / 2.0) / (numPositive * numNegetive)

    val numCorrect = sorted.filter(f => (f._2 >= 0.5 && f._1 == 1) || (f._2 < 0.5 && f._1 == 0)).count()
    val precision = numCorrect * 1.0 / numTotal
    sorted.unpersist()
    return (auc, precision)
  }

  override def train(trainSet: RDD[Instance]): MLModel = {

    val dim = this.dim
    val factor = this.factor
    val reg0 = this.reg0
    val regw = this.regw
    val regv = this.regv
    var learn_rate = this.learn_rate

    val fm = new FMModel(dim, factor)

    def build(iter: Iterator[Instance]): Iterator[(BZS[Double], Int)] = {
      iter.map { case instance =>
        val label = if (instance.label <= 0) 0 else 1
        (new BZS[Double](instance.feature.toSparse.indices, instance.feature.toSparse.values, dim),
        label)
      }
    }

    def log1pExp(x: Double): Double = {
      if (x > 0) {
        x + math.log1p(math.exp(-x))
      } else {
        math.log1p(math.exp(x))
      }
    }

    val data = trainSet.mapPartitions(build)
    data.cache()

    def miniBatch(iter: Iterator[(BZS[Double], Int)]): Iterator[(Double, Int)] = {
      // build feature index
      val featIdx = new IntOpenHashSet()
      val buf = new mutable.ArrayBuffer[((BZS[Double], Int))]()
      while (iter.hasNext) {
        val (feature, label) = iter.next()
        for (elem <- feature.index) {featIdx.add(elem)}
        buf.append((feature, label))
      }

      val (w0, w, v) = fm.pullFromPS(featIdx.toIntArray())

      // initialize gradient
      var w0_grad = 0.0
      val w_grad  = new BZD[Double](dim)
      val v_grad  = new mutable.HashMap[Int, BZD[Double]]()
      v.keySet.foreach(k => v_grad.put(k, new BZD[Double](new Array[Double](factor))))

      val sum_f = new Array[Double](factor)
      var sumLoss = 0.0

      buf.foreach { case (feat, label) =>
        val margin = -1.0 * predictInstance(feat, w0, w, v, sum_f)
        val multiply = (1.0 / (1.0 + Math.exp(margin))) - label

        if (label > 0)
          sumLoss += log1pExp(margin)
        else
          sumLoss += log1pExp(margin) - margin

        // gradient for w0
        w0_grad += multiply + reg0 * w0

        // gradient for w
        for (i <- 0 until feat.activeSize) {
          w_grad(feat.index(i)) += multiply * feat.data(i) + regw * w(feat.index(i))
        }

        // gradient for v
        for (i <- 0 until feat.activeSize) {
          val vector = v(feat.index(i))
          val grad_v = v_grad(feat.index(i))
          for (f <- 0 until factor) {
            val grad = sum_f(f) * feat.data(i) - vector(f) * feat.data(i) * feat.data(i)
            grad_v(f) += multiply * grad + regv * vector(f)
          }
        }
      }

      val batchSize = buf.size
      val scaler = -learn_rate / batchSize

      w0_grad *= scaler
      w_grad *= scaler
      for (elem <- v_grad) elem._2 *= scaler

      fm.pushToPS(w0_grad, w_grad, v_grad)

      Iterator.single((sumLoss, buf.size))
    }

    for (i <- 0 until 100) {
      learn_rate = learn_rate / Math.sqrt(1.0 + 1 * i)
      val (sumLoss, cnt) = data.sample(true, 0.1).mapPartitions(miniBatch)
        .reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))

      val scores = predict(data, fm)
      val (auc, precision) = evaluate(scores)
      println(s"sumLoss=$sumLoss cnt=$cnt loss=${sumLoss / cnt} auc=$auc precision=$precision")

    }

    fm
  }

  override def loadModel(modelPath: String) = ???
}
