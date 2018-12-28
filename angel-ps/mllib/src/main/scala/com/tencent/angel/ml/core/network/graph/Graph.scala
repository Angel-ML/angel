package com.tencent.angel.ml.core.network.graph

import com.tencent.angel.client.AngelClient
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.graph
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.variable.Variable
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.matrix.Matrix
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.ListBuffer

class TimeStats(
                 var forwardTime: Long = 0,
                 var backwardTime: Long = 0,
                 var calGradTime: Long = 0,
                 var pullParamsTime: Long = 0,
                 var pushParamsTime: Long = 0,
                 var updateTime: Long = 0) extends Serializable {
  val LOG: Log = LogFactory.getLog(classOf[TimeStats])

  def summary(): String = {
    val summaryString = s"\nSummary: \n\t" +
      s"forwardTime = $forwardTime, \n\tbackwardTime = $backwardTime, \n\t" +
      s"calGradTime = $calGradTime, \n\tpullParamsTime = $pullParamsTime, \n\t" +
      s"pushParamsTime = $pushParamsTime, \n\tupdateTime = $updateTime"

    LOG.info(summaryString)
    summaryString
  }
}


trait EvnContext

case class AngelEvnContext(client: AngelClient) extends EvnContext

case class LocalEvnContext() extends EvnContext


abstract class Graph(val placeHolder: PlaceHolder, val conf: SharedConf) {
  def this(placeHolder: PlaceHolder) = this(placeHolder, SharedConf.get())

  private val LOG: Log = LogFactory.getLog(classOf[Graph])

  protected val inputLayers = new ListBuffer[InputLayer]()
  protected var lossLayer: LossLayer = _
  protected val trainableLayer = new ListBuffer[Trainable]()
  var taskNum: Int = _

  @transient protected val variables = new ListBuffer[Variable]()
  val timeStats = new graph.TimeStats()

  def addVariable(v: Variable): Unit = {
    variables.append(v)
  }

  def getVariables: List[Variable] = variables.toList

  def addInput(layer: InputLayer): Unit = {
    inputLayers.append(layer)
  }

  def setOutput(layer: LossLayer): Unit = {
    lossLayer = layer
  }

  def getOutputLayer: LossLayer = lossLayer

  def addTrainable(layer: Trainable): Unit = {
    trainableLayer.append(layer)
  }

  def getTrainable: List[Trainable] = {
    trainableLayer.toList
  }

  def getLossLayer: LossLayer = lossLayer

  protected def deepFirstDown(layer: Layer)(predicate: Layer => Boolean, action: Layer => Unit): Unit = {
    if (predicate(layer)) {
      action(layer)
      layer.input.foreach { lowerLayer =>
        deepFirstDown(lowerLayer)(predicate, action)
      }
    }
  }

  def setState(predicate: Layer => Boolean, status: STATUS.STATUS): Unit = {
    deepFirstDown(lossLayer.asInstanceOf[Layer])(
      predicate, (layer: Layer) => layer.status = status
    )
  }

  def feedData(data: Array[LabeledData]): Unit = {
    deepFirstDown(lossLayer.asInstanceOf[Layer])(
      (lay: Layer) => lay.status != STATUS.Null,
      (lay: Layer) => lay.status = STATUS.Null
    )

    placeHolder.feedData(data)
  }

  def predict(): Matrix = {
    val start = System.currentTimeMillis()
    val res = lossLayer.predict()
    timeStats.forwardTime += (System.currentTimeMillis() - start)
    res
  }

  def calLoss(): Double = lossLayer.calLoss()

  def calBackward(): Unit = {
    val start = System.currentTimeMillis()
    inputLayers.foreach { layer => layer.calBackward() }
    timeStats.backwardTime += (System.currentTimeMillis() - start)
  }

  def pullParams(epoch: Int): Unit = {
    val start = System.currentTimeMillis()
    trainableLayer.foreach { layer => layer.pullParams(epoch: Int) }
    timeStats.pullParamsTime += (System.currentTimeMillis() - start)
  }

  def pushGradient(): Unit = {
    val start = System.currentTimeMillis()
    trainableLayer.foreach(layer => layer.pushGradient())
    timeStats.pushParamsTime += (System.currentTimeMillis() - start)
  }

  def update(epoch: Int, batchSize: Int): Unit = {
    val start = System.currentTimeMillis()
    val updateFuture = trainableLayer.map(layer => layer.update(epoch, batchSize))
    for (future <- updateFuture) future.get
    timeStats.updateTime += (System.currentTimeMillis() - start)
  }

  def setLR(lr: Double): Unit = {
    trainableLayer.foreach { trainable =>
      trainable.optimizer.setLR(lr)
    }
  }

  def getNormal: Double = {
    conf.get(AngelConf.ANGEL_RUNNING_MODE) match {
      case "ANGEL_PS" => 1.0
      case "ANGEL_PS_WORKER" => placeHolder.getBatchSize * taskNum
      case "ANGEL_LOCAL" => placeHolder.getBatchSize
    }
  }

  def init(taskId: Int = 0): Unit = {
    trainableLayer.foreach { layer => layer.init(taskId) }
  }

  override def toString: String = {
    val str = new StringBuilder
    deepFirstDown(lossLayer.asInstanceOf[Layer])(_ => true, layer => str.append(layer.toString + "\n"))
    str.toString()
  }

  /**
    * Create matrices contain in the model, this method is only used in Driver/Client
    *
    * @param evnCtx EvnContext
    */
  def createMatrices(envCtx: EvnContext): Unit

  /**
    * Load model from files, this method is only used in Driver/Client
    *
    * @param evnCtx EvnContext
    */
  def loadModel(envCtx: EvnContext, path: String): Unit

  /**
    * Create matrices contain in the model, this method is only used in Worker/Executor
    */
  def createMatrices(): Unit

  /**
    * Save model to files, this method is only use in Driver/Client
    *
    * @param evnCtx EvnContext
    */
  def saveModel(envCtx: EvnContext, path: String): Unit

}
