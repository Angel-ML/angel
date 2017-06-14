package com.tencent.angel.example.quickStart

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, TDoubleVector}
import com.tencent.angel.ml.optimizer.sgd.L2LogLoss
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.worker.task.{TaskContext, TrainTask}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * Created by yunkuntan on 2017/6/11.
  */
class myLRTrainTask (val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx){
  val LOG: Log = LogFactory.getLog(classOf[myLRTrainTask])

  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  val lr: Double = conf.getDouble(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEAR_RATE)
  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  val dataFormat = conf.get(MLConf.ML_DATAFORMAT, MLConf.DEFAULT_ML_DATAFORMAT)
  val reg: Double = conf.getDouble(MLConf.ML_REG_L2, MLConf.DEFAULT_ML_REG_L2)
  val loss = new L2LogLoss(reg)

  /**
    * Parse each sample into a labeled data, of which X is the feature weight vector, Y is label.
    */
  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    DataParser.parseVector(key, value, feaNum, dataFormat, negY = true)
  }

  override
  def train(ctx: TaskContext): Unit = {

    // A simple logistic regression model
    val model = new myLRModel(ctx, conf)

    // Apply batch gradient descent LR iteratively
    while (ctx.getIteration < epochNum) {
      // Pull model from PS Server
      val weight = model.weight.getRow(0)

      // Calculate gradient vector
      val grad = bathGradientDescent(weight)

      // Push gradient vector to PS Server
      model.weight.increment(grad.timesBy(-1.0 * lr))

      // LR model matrix clock
      model.weight.clock.get

      // Increase iteration number
      ctx.incIteration()
    }
  }

  def bathGradientDescent(weight: TDoubleVector): TDoubleVector = {

    val grad = new DenseDoubleVector(feaNum)
    var batchLoss = 0.0

    dataBlock.resetReadIndex()
    for (i <- 0 until dataBlock.getTotalElemNum) {
      val data = dataBlock.read()
      val x = data.getX
      val y = data.getY

      val pre = weight.dot(x)
      val gradScalar = loss.grad(pre, y)
      grad.plusBy(x, -1.0 * gradScalar)
      batchLoss += loss.loss(pre, y)
    }

    grad.timesBy(1.toDouble / dataBlock.getTotalElemNum)

    for (index <- 0 until grad.size) {
      if (grad.get(index) > 10e-7) {
        grad.set(index, grad.get(index) + weight.get(index) * (loss.getRegParam))
      }
    }

    grad.setRowId(0)

    LOG.info(s"Gradient descent batch ${ctx.getIteration}, batch loss=$batchLoss")
    grad
  }

}
