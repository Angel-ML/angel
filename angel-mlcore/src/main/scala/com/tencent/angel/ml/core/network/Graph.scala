package com.tencent.angel.ml.core.network

import com.tencent.angel.ml.core.data.LabeledData
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.variable.{Variable, VariableProvider}
import com.tencent.angel.ml.core.utils.{Callback, RowTypeUtils}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType
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


trait KVSType {
  val modelType: RowType

  def storageType: String = RowTypeUtils.storageType(modelType)

  def valueType: String = RowTypeUtils.valueType(modelType)

  def keyType: String = RowTypeUtils.keyType(modelType)
}


abstract class Graph(val placeHolder: PlaceHolder, val providerName: String) extends KVSType {
  private val LOG: Log = LogFactory.getLog(classOf[Graph])

  protected val inputLayers = new ListBuffer[InputLayer]()
  protected var lossLayer: LossLayer = _
  protected val trainableLayer = new ListBuffer[Trainable]()
  @transient protected val variables = new ListBuffer[Variable]()

  val timeStats = new TimeStats()

  val provider: VariableProvider = {
    val cls = Class.forName(providerName)
    val constructor = cls.getConstructor(classOf[Graph])
    val instance = constructor.newInstance(this)

    instance.asInstanceOf[VariableProvider]
  }

  var taskNum: Int

  val indexRange: Long

  val validIndexNum: Long

  def normalFactor: Double

  val dataFormat: String

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

  def update[T](epoch: Int, batchSize: Int): Unit = {
    val start = System.currentTimeMillis()
    val callbacks = trainableLayer.map { layer =>
      val callback = new Callback[T]()
      layer.update[T](epoch, batchSize)(callback)
    }
    for (callback <- callbacks) callback.get()
    timeStats.updateTime += (System.currentTimeMillis() - start)
  }

  def setLR(lr: Double): Unit = {
    trainableLayer.foreach { trainable =>
      trainable.optimizer.setLR(lr)
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
