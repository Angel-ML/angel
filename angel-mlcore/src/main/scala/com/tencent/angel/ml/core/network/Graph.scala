package com.tencent.angel.ml.core.network

import com.tencent.angel.ml.core.PredictResult
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.layers.{Trainable, _}
import com.tencent.angel.ml.core.variable.{Variable, VariableManager, VariableProvider}
import com.tencent.angel.ml.core.optimizer.loss.LossFunc
import com.tencent.angel.ml.core.utils.JsonUtils.{J2Pretty, layer2Json}
import com.tencent.angel.ml.core.utils.{DataCache, RowTypeUtils, TimeStats}
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.{LabeledData, RowType}
import org.apache.commons.logging.{Log, LogFactory}
import org.json4s.JsonAST.{JField, JObject}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


trait EvnContext


trait KVSType {
  val modelType: RowType

  def keyType: String = RowTypeUtils.keyType(modelType)

  def valueType: String = RowTypeUtils.valueType(modelType)

  def storageType: String = RowTypeUtils.storageType(modelType)
}


abstract class Graph(val placeHolder: PlaceHolder, val providerName: String) extends KVSType {
  private val LOG: Log = LogFactory.getLog(classOf[Graph])

  protected val inputLayers = new ListBuffer[InputLayer]()
  protected var lossLayer: LossLayer = _
  protected val trainableLayer = new ListBuffer[Trainable]()
  private val dataCache = new DataCache()
  protected val variableManager: VariableManager

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

  protected var lr: Double = SharedConf.learningRate

  def addInputLayer(layer: InputLayer): Unit = {
    inputLayers.append(layer)
  }

  def getALLInputLayers: List[InputLayer] = inputLayers.toList

  def getInputLayer(name: String): InputLayer = {
    val layerOption = inputLayers.collectFirst {
      case layer: InputLayer if layer.name == name => layer
    }

    layerOption.getOrElse(null.asInstanceOf[InputLayer])
  }

  def setLossLayer(layer: LossLayer): Unit = {
    lossLayer = layer
  }

  def getLossLayer: LossLayer = lossLayer

  def getLossFunc: LossFunc = lossLayer.lossFunc

  def addTrainableLayer(layer: Trainable): Unit = {
    trainableLayer.append(layer)
  }

  def getALLTrainableLayers: List[Trainable] = {
    trainableLayer.toList
  }

  def getTrainableLayer(name: String): Trainable = {
    val trainableOption = trainableLayer.collectFirst {
      case layer: Layer if layer.name == name => layer.asInstanceOf[Trainable]
    }

    trainableOption.getOrElse(null.asInstanceOf[Trainable])
  }

  protected def deepFirstDown(layer: Layer)(predicate: Layer => Boolean, action: Layer => Unit): Unit = {
    if (predicate(layer)) {
      action(layer)
      layer match {
        case l: JoinLayer =>
          l.inputLayers.foreach { lowerLayer =>
            deepFirstDown(lowerLayer)(predicate, action)
          }
        case l: LinearLayer => deepFirstDown(l.inputLayer)(predicate, action)
        case _: InputLayer =>
      }
    }
  }


  /** **********************************************************************************
    * training
    */

  def setLR(lr: Double): Unit = {
    this.lr = lr
    trainableLayer.foreach { trainable =>
      trainable.optimizer.setLR(lr)
    }
  }

  def feedData(data: Array[LabeledData]): Unit = {
    placeHolder.feedData(data)
  }

  // forward
  def calForward(): Double = {
    val start = System.currentTimeMillis()
    clearCache()
    val loss = lossLayer.calLoss()
    val end = System.currentTimeMillis()

    timeStats.forwardTime += end - start

    loss
  }

  // backward
  def calBackward(): Unit = {
    val start = System.currentTimeMillis()
    inputLayers.foreach { layer => layer.backward(layer) }
    val end = System.currentTimeMillis()

    timeStats.backwardTime += end - start
  }


  /** **********************************************************************************
    * predict
    */

  def predict(): List[PredictResult] = {
    val start = System.currentTimeMillis()
    clearCache()
    val res = lossLayer.predict()
    val end = System.currentTimeMillis()
    timeStats.predictTime += end - start

    res
  }


  /** **********************************************************************************
    * Variable operation
    */

  def addVariable(v: Variable): Unit = variableManager.addVariable(v)

  def getALLVariables: List[Variable] = variableManager.getALLVariables

  def getVariable(name: String): Variable = variableManager.getVariable(name)

  def hasVariable(v: Variable): Boolean = variableManager.hasVariable(v)

  def hasVariable(name: String): Boolean = variableManager.hasVariable(name)

  def putGradient(v: Variable, g: Matrix): Unit = variableManager.putGradient(v, g)

  def getAllGradients: Map[String, Matrix] = variableManager.getAllGradients

  def getGradient(name: String): Matrix = variableManager.getGradient(name)

  def hasGradient(name: String): Boolean = variableManager.hasGradient(name)

  //---------------------Training Cycle
  def createMatrices(envCtx: EvnContext): Unit = {
    val start = System.currentTimeMillis()
    variableManager.createALL(envCtx)
    val end = System.currentTimeMillis()

    timeStats.createTime += end - start
  }

  def init(taskId: Int = 0): Unit = {
    val start = System.currentTimeMillis()
    variableManager.initALL(taskId)
    val end = System.currentTimeMillis()

    timeStats.initTime += end - start
  }

  def pullParams(epoch: Int): Unit = {
    val start = System.currentTimeMillis()
    variableManager.pullALL(epoch)
    val end = System.currentTimeMillis()

    timeStats.pullParamsTime += end - start
  }

  def pushGradient(): Unit = {
    val start = System.currentTimeMillis()
    variableManager.pushALL(lr)
    val end = System.currentTimeMillis()

    timeStats.pushParamsTime += end - start
  }

  def update[T](epoch: Int, batchSize: Int): Unit = {
    val start = System.currentTimeMillis()
    variableManager.updateALL[T](epoch, batchSize)
    val end = System.currentTimeMillis()

    timeStats.updateTime += end - start
  }

  def loadModel(envCtx: EvnContext, path: String): Unit = {
    val start = System.currentTimeMillis()
    variableManager.loadALL(envCtx, path)
    val end = System.currentTimeMillis()

    timeStats.loadTime += end - start
  }

  def saveModel(envCtx: EvnContext, path: String): Unit = {
    val start = System.currentTimeMillis()
    variableManager.saveALL(envCtx, path)
    val end = System.currentTimeMillis()

    timeStats.saveTime += end - start
  }

  /** **********************************************************************************
    * Matrix Cache
    */

  def put2Cache(name: String, mat: Matrix): Unit = {
    dataCache.addMatrix(name, mat)
  }

  def put2Cache(name: String, vec: Vector): Unit = {
    dataCache.addVector(name, vec)
  }

  def isMatrixInCache(name: String): Boolean = {
    dataCache.hasMatrix(name)
  }

  def isVectorInCache(name: String): Boolean = {
    dataCache.hasVector(name)
  }

  def getMatrixFromCache(name: String): Matrix = {
    dataCache.getMatrix(name)
  }

  def getVectorFromCache(name: String): Vector = {
    dataCache.getVector(name)
  }

  def clearCache(): Unit = {
    dataCache.clearAll()
  }

  /** **********************************************************************************
    * toString/toJson
    */

  override def toString: String = {
    val str = new StringBuilder
    deepFirstDown(lossLayer.asInstanceOf[Layer])(_ => true, layer => str.append(layer.toString + "\n"))
    str.toString()
  }

  def toJson: String = {
    implicit val jsonMap: mutable.HashMap[String, JField] = new mutable.HashMap[String, JField]()
    layer2Json(lossLayer.asInstanceOf[Layer])
    J2Pretty(JObject(jsonMap.values.toList))
  }
}
