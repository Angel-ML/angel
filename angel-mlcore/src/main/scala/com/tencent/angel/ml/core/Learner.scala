package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.data.{DataBlock, LabeledData}
import com.tencent.angel.ml.core.network.Graph
import org.apache.commons.logging.LogFactory
import com.tencent.angel.ml.core.optimizer.decayer.StepSizeScheduler

import scala.collection.mutable

trait Learner {
  private val LOG = LogFactory.getLog(classOf[Learner])

  val model: Model
  val graph: Graph
  protected val ssScheduler: StepSizeScheduler

  protected def barrier(graph: Graph): Unit

  protected val preHook: mutable.ListBuffer[Learner.HookFunc] = new mutable.ListBuffer[Learner.HookFunc]()
  protected val postHook: mutable.ListBuffer[Learner.HookFunc] = new mutable.ListBuffer[Learner.HookFunc]()

  def addPreHook(func: Learner.HookFunc): Unit = {
    preHook.append(func)
  }

  def addPostHook(func: Learner.HookFunc): Unit = {
    postHook.append(func)
  }

  protected def trainOneEpoch(epoch: Int, iter: Iterator[Array[LabeledData]], numBatch: Int): Double

  def train(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]): Model = {
    train(trainData, null, validationData)
  }

  def train(posTrainData: DataBlock[LabeledData],
            negTrainData: DataBlock[LabeledData],
            validationData: DataBlock[LabeledData]): Model

  protected def validate(epoch: Int, valiData: DataBlock[LabeledData]): Unit
}

object Learner {
  type HookFunc = Graph => Unit
}
