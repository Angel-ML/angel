package com.tencent.angel.ml.factorizationmachines

import java.util

import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, SparseDoubleSortedVector, SparseDoubleVector, TDoubleVector}
import com.tencent.angel.ml.metric.log.LossMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}

import scala.util.Random

class FMLearner(override val ctx: TaskContext) extends MLLearner(ctx) {
  val LOG: Log = LogFactory.getLog(classOf[FMLearner])
  val fmmodel = new FMModel(conf, ctx)

  val learnType = conf.get(MLConf.ML_FM_LEARN_TYPE, MLConf.DEFAULT_ML_FM_LEARN_TYPE)
  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)

  val rank: Int = conf.getInt(MLConf.ML_FM_RANK, MLConf.DEFAULT_ML_FM_RANK)
  val reg0: Double = conf.getDouble(MLConf.ML_FM_REG0, MLConf.DEFAULT_ML_FM_REG0)
  val reg1: Double = conf.getDouble(MLConf.ML_FM_REG1, MLConf.DEFAULT_ML_FM_REG1)
  val reg2: Double = conf.getDouble(MLConf.ML_FM_REG2, MLConf.DEFAULT_ML_FM_REG2)
  val lr: Double = conf.getDouble(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val vInit: Double = conf.getDouble(MLConf.ML_FM_V_INIT, MLConf.DEFAULT_ML_FM_V_INIT)

  var maxP = 0.0
  var minP = 0.0

  val vIndexs = new Array[Int](feaNum)
  for (i <- 0 until feaNum)
    vIndexs(i) = i

  /**
    * Train a ML Model
    *
    * @param trainData : input train data storage
    * @param vali      : validate data storage
    * @return : a learned model
    */
  override
  def train(trainData: DataBlock[LabeledData], vali: DataBlock[LabeledData]): MLModel = {
    val start = System.currentTimeMillis()
    LOG.info(s"learnType=$learnType, feaNum=$feaNum, rank=$rank, #trainData=${trainData.size}")
    LOG.info(s"reg0=$reg0, reg1=$reg1, reg2=$reg2, lr=$lr, initV=$vInit")

    val beforeInit = System.currentTimeMillis()
    initModels()
    val initCost = System.currentTimeMillis() - beforeInit
    LOG.info(s"Init matrixes cost $initCost ms.")

    val ran = rangeOfPredict(trainData)
    minP = ran._1
    maxP = ran._2

    globalMetrics.addMetrics(fmmodel.FM_OBJ, LossMetric(trainData.size()))

    while (ctx.getIteration < epochNum) {
      val startIter = System.currentTimeMillis()
      val (w0, w, v) = oneIteration(trainData)
      val iterCost = System.currentTimeMillis() - startIter

      val startVali = System.currentTimeMillis()
      val loss = evaluate(trainData, w0.get(0), w, v)
      val valiCost = System.currentTimeMillis() - startVali

      globalMetrics.metrics(fmmodel.FM_OBJ, loss)
      LOG.info(s"Epoch=${ctx.getIteration}, evaluate loss=${loss/trainData.size()}. " +
        s"trainCost=$iterCost, " +
        s"valiCost=$valiCost")

      ctx.incIteration()
    }

    val end = System.currentTimeMillis()
    val cost = end - start
    LOG.info(s"FM Learner train cost $cost ms.")
    fmmodel
  }

  def initModels(): Unit = {
    val totalTask = ctx.getTotalTaskNum
    val taskId = ctx.getTaskId.getIndex
    val random = new Random()

    for (row <- 0 until feaNum) {
      if (row % totalTask == taskId) {
        val randV = new DenseDoubleVector(rank);
        randV.setRowId(row)

        for (col <- 0 until rank) {
          val rand = math.random
          randV.set(col, vInit * random.nextGaussian())
        }

        fmmodel.v.increment(randV)
      }
    }

    fmmodel.v.clock().get()
  }

  def oneIteration(dataBlock: DataBlock[LabeledData]): (DenseDoubleVector,
    DenseDoubleVector, util.List[DenseDoubleVector]) = {
    val startGet = System.currentTimeMillis()
    val (w0, w, v) = fmmodel.pullFromPS(vIndexs)
    val getCost = System.currentTimeMillis() - startGet
    LOG.info(s"Get matrixes cost $getCost ms.")

    val _w0 = w0.clone()
    val _w = w.clone()
    val _v = new util.ArrayList[DenseDoubleVector]
    for (i <- 0 until feaNum) {
      _v.add(v.get(i).clone())
    }

    dataBlock.resetReadIndex()
    for (_ <- 0 until dataBlock.size) {
      val data = dataBlock.read()
      val x = data.getX.asInstanceOf[SparseDoubleSortedVector]
      val y = data.getY
      val pre = predict(x, y, _w0.get(0), _w, _v)
      val dm = derviationMultipler(y, pre)

      _w0.add(0, -lr * (dm + reg0 * _w0.get(0)))
      _w.timesBy(1 - lr * reg1).plusBy(x, -lr * dm)
      updateV(x, dm, _v)
    }

    for (i <- 0 until feaNum) {
      v.get(i).plusBy(_v.get(i), -1.0).timesBy(-1.0)
    }

    fmmodel.pushToPS(w0.plusBy(_w0, -1.0).timesBy(-1.0).asInstanceOf[DenseDoubleVector],
      w.plusBy(_w, -1.0).timesBy(-1.0).asInstanceOf[DenseDoubleVector],
      v)

    (_w0, _w, _v)
  }


  def evaluate(dataBlock: DataBlock[LabeledData], w0: Double, w: DenseDoubleVector,
               v: util.List[DenseDoubleVector]):
  Double = {
    var loss = 0.0

    dataBlock.resetReadIndex()
    for (_ <- 0 until dataBlock.size) {
      val data = dataBlock.read()
      val x = data.getX.asInstanceOf[SparseDoubleSortedVector]
      val y = data.getY
      val pre = predict(x, y, w0, w, v)
      loss += (pre - y) * (pre - y)
    }

    loss
  }

  def predict(x: SparseDoubleSortedVector, y: Double, w0: Double, w: DenseDoubleVector, v:
  util.List[DenseDoubleVector]): Double = {
    var ret: Double = 0.0
    ret += w0
    ret += x.dot(w)

    for (f <- 0 until rank) {
      var ret1 = 0.0
      var ret2 = 0.0
      for (i <- 0 until x.size()) {
        val tmp = x.getValues()(i) * v.get(x.getIndices()(i)).get(f)
        ret1 += tmp
        ret2 += tmp * tmp
      }
      ret += 0.5 * (ret1 * ret1 - ret2)
    }

    ret = if (ret < maxP) ret else maxP
    ret = if (ret > minP) ret else minP

    ret
  }

  // \frac{\partial loss}{\partial x} = dm * \frac{\partial y}{\partial x}
  def derviationMultipler(y: Double, pre: Double): Double = {
    learnType match {
      // For classification:
      // loss=-ln(\delta (pre\cdot y))
      // \frac{\partial loss}{\partial x}=(\delta (pre\cdot y)-1)y\frac{\partial y}{\partial x}
      case "c" => -y * (1.0 - 1.0 / (1 + Math.exp(-y * pre)))
      // For regression:
      // loss = (pre-y)^2
      // \frac{\partial loss}{\partial x}=2(pre-y)\frac{\partial y}{\partial x}
      //      case "r" => 2 * (pre - y)
      case "r" => pre - y
    }
  }

  def updateV(x: SparseDoubleSortedVector, dm: Double, v: util.List[DenseDoubleVector]): Unit = {

    for (f <- 0 until rank) {
      // calculate dot(vf, x)
      var dot = 0.0
      for (i <- 0 until x.size()) {
        dot += x.getValues()(i) * v.get(x.getIndices()(i)).get(f)
      }

      for (i <- 0 until x.size()) {
        val j = x.getIndices()(i)
        val grad = dot * x.getValues()(i) - v.get(j).get(f) * x.getValues()(i) * x.getValues()(i)
        v.get(j).add(f, -lr * (dm * grad + reg2 * v.get(j).get(f)))
      }
    }
  }

  def rangeOfPredict(trainData: DataBlock[LabeledData]): (Double, Double) = {
    var maxP = -Double.MaxValue
    var minP = Double.MaxValue

    trainData.resetReadIndex()
    for (i <- 0 until trainData.size()) {
      val y = trainData.read().getY
      maxP = if (maxP > y) maxP else y
      minP = if (minP < y) minP else y
    }

    (minP, maxP)
  }
}