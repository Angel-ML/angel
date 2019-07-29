package com.tencent.angel.ml.core.network.layers

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.utils.LayerKeys
import com.tencent.angel.ml.servingmath2.matrix.Matrix
import org.json4s.JsonAST.{JArray, JField, JString}
import org.json4s.JsonDSL._


abstract class JoinLayer(name: String, outputDim: Int, val inputLayers: Array[Layer])(implicit graph: Graph)
  extends Layer(name, outputDim) {
  inputLayers.foreach { layer =>
    layer.addConsumer(this)
    this.addInput(layer)
  }

  override def forward(): Matrix = {
    if (graph.isMatrixInCache(forwardKey)) {
      graph.getMatrixFromCache(forwardKey)
    } else {
      var usedNames = Set[String]()
      val inputs = inputLayers.map { layer =>
//        layer.name -> layer.forward()
        val name = layer.name
        val newName = if (usedNames.contains(name)) {
          name + "_x"
        } else {
          usedNames += name
          name
        }
        newName -> layer.forward()
      }.toMap
      val forwardValue: Matrix = doForward(inputs)
      graph.put2Cache(forwardKey, forwardValue)
      forwardValue
    }
  }

  protected def doForward(inputs: Map[String, Matrix]): Matrix

  override def backward(layer: Layer): Matrix = {
    assert(isInput(layer))
    if (graph.isMatrixInCache(backwardKey)) {
      val key = s"$backwardKey/${layer.name}"
      graph.getMatrixFromCache(key)
    } else {
      val gradInput = gatherGradInput()
      var usedNames = Set[String]()
      val inputs = inputLayers.map { layer =>
//        layer.name -> layer.forward()
        val name = layer.name
        val newName = if (usedNames.contains(name)) {
          name + "_x"
        } else {
          usedNames += name
          name
        }
        newName -> layer.forward()
      }.toMap
      val matrixMap = doBackward(inputs, gradInput)
      graph.put2Cache(backwardKey, null.asInstanceOf[Matrix])
      matrixMap.foreach { case (layerName: String, mat: Matrix) =>
        val key = s"$backwardKey/$layerName"
        graph.put2Cache(key, mat)
      }

      matrixMap(layer.name)
    }
  }

  protected def doBackward(inputs: Map[String, Matrix], gradInput: Matrix): Map[String, Matrix]

  override def toJson: JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim) ~
      (LayerKeys.inputLayersKey -> JArray(inputLayers.toList.map(layer => JString(layer.name))))

    JField(name, layerJson)
  }


}
