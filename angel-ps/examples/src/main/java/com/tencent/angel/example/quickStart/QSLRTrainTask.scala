package com.tencent.angel.example.quickstart

import com.tencent.angel.ml.conf.MLConf._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, TDoubleVector}
import com.tencent.angel.ml.optimizer.sgd.L2LogLoss
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.worker.task.{TaskContext, TrainTask}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.io.{LongWritable, Text}


class QSLRTrainTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx){
  val LOG: Log = LogFactory.getLog(classOf[QSLRTrainTask])

  val epochNum = conf.getInt(ML_EPOCH_NUM, DEFAULT_ML_EPOCH_NUM)
  val lr = conf.getDouble(ML_LEARN_RATE, DEFAULT_ML_LEAR_RATE)
  val feaNum = conf.getInt(ML_FEATURE_NUM, DEFAULT_ML_FEATURE_NUM)
  val dataFmt = conf.get(ML_DATAFORMAT, DEFAULT_ML_DATAFORMAT)
  val reg = conf.getDouble(ML_REG_L2, DEFAULT_ML_REG_L2)
  val loss = new L2LogLoss(reg)

  /**
    * Parse each sample into a labeled data, of which X is the feature weight vector, Y is label.
    */
  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    DataParser.parseVector(key, value, feaNum, dataFmt, negY = true)
  }

  override
  def train(ctx: TaskContext): Unit = {
    // A simple logistic regression model
    val model = new QSLRModel(conf, ctx)
    // Apply batch gradient descent LR iteratively
    while (ctx.getEpoch < epochNum) {
      // Get model and calculate gradient
      val weight = model.weight.getRow(0)
      val grad = batchGradientDescent(weight)

      // Push gradient vector to PS Server and clock
      model.weight.increment(grad.timesBy(-1.0 * lr))
      model.weight.syncClock()

      // Increase iteration number
      ctx.incEpoch()
    }
  }

  def batchGradientDescent(weight: TDoubleVector): TDoubleVector = {
    var (grad, batchLoss) = (new DenseDoubleVector(feaNum), 0.0)

    // Iterating samples, calculates the loss and summate the grad
    trainDataBlock.resetReadIndex()
    for (i <- 0 until trainDataBlock.size) {
      val data = trainDataBlock.read()
      val pred = weight.dot(data.getX)
      grad.plusBy(data.getX, -1.0 * loss.grad(pred, data.getY))
      batchLoss += loss.loss(pred, data.getY)
    }
    grad.timesBy((1 + reg) * 1.0 / trainDataBlock.size)

    LOG.info(s"Gradient descent batch ${ctx.getEpoch}, batch loss=$batchLoss")
    grad.setRowId(0)
    grad
  }

}
