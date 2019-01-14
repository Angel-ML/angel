package com.tencent.angel.ml.core.network

import com.tencent.angel.ml.core.PredictResult
import com.tencent.angel.ml.core.data.LabeledData
import com.tencent.angel.ml.core.network.layers.{Trainable, _}
import com.tencent.angel.ml.core.network.variable.{Variable, VariableProvider}
import com.tencent.angel.ml.core.utils.JsonUtils.{J2Pretty, layer2Json}
import com.tencent.angel.ml.core.utils.{Callback, RowTypeUtils}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType
import org.apache.commons.logging.{Log, LogFactory}
import org.json4s.JsonAST.{JField, JObject}

import scala.collection.mutable
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

  lazy val provider: VariableProvider = {
    val cls = Class.forName(providerName)
    val constructor = cls.getConstructor(classOf[Graph])
    val instance = constructor.newInstance(this)

    instance.asInstanceOf[VariableProvider]
  }

  val taskNum: Int

  val indexRange: Long

  val validIndexNum: Long

  def normalFactor: Double

  val dataFormat: String

  def addVariable(v: Variable): Unit = {
    variables.append(v)
  }

  def getALLVariables: List[Variable] = variables.toList

  def getVariable(idx: Int): Variable = {
    if (idx>=0 && idx < variables.size) {
      variables(idx)
    } else {
      null.asInstanceOf[Variable]
    }
  }

  def getVariable(name: String): Variable = {
    val variableOption = variables.collectFirst{
      case variable: Variable if variable.name == name => variable
    }

    variableOption.getOrElse(null.asInstanceOf[Variable])
  }

  def addInputLayer(layer: InputLayer): Unit = {
    inputLayers.append(layer)
  }

  def getALLInputLayers: List[InputLayer] = inputLayers.toList

  def getInputLayer(idx: Int): InputLayer = {
    if (idx >=0 && idx < inputLayers.size) {
      inputLayers(idx)
    } else {
      null.asInstanceOf[InputLayer]
    }
  }

  def getInputLayer(name: String): InputLayer = {
    val layerOption = inputLayers.collectFirst{
      case layer: InputLayer if layer.name == name => layer
    }

    layerOption.getOrElse(null.asInstanceOf[InputLayer])
  }

  def setLossLayer(layer: LossLayer): Unit = {
    lossLayer = layer
  }

  def getLossLayer: LossLayer = lossLayer

  def addTrainableLayer(layer: Trainable): Unit = {
    trainableLayer.append(layer)
  }

  def getALLTrainableLayers: List[Trainable] = {
    trainableLayer.toList
  }

  def getTrainableLayer(idx: Int): Trainable = {
    if (idx >=0 && idx < trainableLayer.size){
      trainableLayer(idx)
    } else {
      null.asInstanceOf[Trainable]
    }
  }

  def getTrainableLayer(name: String): Trainable = {
    val trainableOption = trainableLayer.collectFirst{
      case layer: Layer if layer.name == name => layer.asInstanceOf[Trainable]
    }

    trainableOption.getOrElse(null.asInstanceOf[Trainable])
  }

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

  def predict(): List[PredictResult] = {
    val start = System.currentTimeMillis()
    val res = lossLayer.predict()
    timeStats.forwardTime += (System.currentTimeMillis() - start)
    res
  }

  def calLoss(): Double = lossLayer.calLoss()

  def init(taskId: Int = 0): Unit = {
    trainableLayer.foreach { layer => layer.init(taskId) }
  }

  def pullParams(epoch: Int): Unit = {
    val start = System.currentTimeMillis()
    trainableLayer.foreach { layer => layer.pullParams(epoch: Int) }
    timeStats.pullParamsTime += (System.currentTimeMillis() - start)
  }

  def calBackward(): Unit = {
    val start = System.currentTimeMillis()
    inputLayers.foreach { layer => layer.calBackward() }
    timeStats.backwardTime += (System.currentTimeMillis() - start)
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
    for (callback <- callbacks) {
      callback.get()
    }
    timeStats.updateTime += (System.currentTimeMillis() - start)
  }

  def setLR(lr: Double): Unit = {
    trainableLayer.foreach { trainable =>
      trainable.optimizer.setLR(lr)
    }
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

  private def layer2Json(topLayer: Layer)(implicit jMap: mutable.HashMap[String, JField]): Unit = {
    topLayer match {
      case l: InputLayer =>
        if (!jMap.contains(l.name)) {
          jMap.put(l.name, l.toJson())
        }
      case l: LinearLayer =>
        if (!jMap.contains(l.name)) {
          jMap.put(l.name, l.toJson())
        }
        layer2Json(l.inputLayer)(jMap)
      case l: JoinLayer =>
        if (!jMap.contains(l.name)) {
          jMap.put(l.name, l.toJson())
        }
        l.inputLayers.foreach(layer => layer2Json(layer)(jMap))
    }
  }

  def toJson(): String = {
    implicit val jsonMap: mutable.HashMap[String, JField] = new mutable.HashMap[String, JField]()
    layer2Json(lossLayer.asInstanceOf[Layer])
    J2Pretty(JObject(jsonMap.values.toList))
  }
}
