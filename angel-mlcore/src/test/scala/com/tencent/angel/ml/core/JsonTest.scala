package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.local.LocalGraph
import com.tencent.angel.ml.core.local.optimizer.{Adam, Momentum, SGD}
import com.tencent.angel.ml.core.network._
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.layers.join.SumPooling
import com.tencent.angel.ml.core.network.layers.linear.FCLayer
import com.tencent.angel.ml.core.network.layers.verge.{Embedding, SimpleInputLayer, SimpleLossLayer}
import com.tencent.angel.ml.core.optimizer.loss._
import com.tencent.angel.ml.core.utils.JsonUtils
import org.scalatest.FunSuite
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._



class JsonTest extends FunSuite{
  test("Adam") {
    val adma = new Adam(0.001, 0.9, 0.99)

    val json = adma.toJson

    val jsonStr = compact(render(json))
    println(jsonStr)

    val jsonRe = parse(jsonStr).asInstanceOf[JObject]

    Adam.fromJson(jsonRe)
  }

  test("Momentum") {
    val moment = new Momentum(0.001, 0.9)

    val json = moment.toJson

    val jsonStr = compact(render(json))
    println(jsonStr)

    val jsonRe = parse(jsonStr).asInstanceOf[JObject]

    Momentum.fromJson(jsonRe)
  }

  test("SGD") {
    val moment = new SGD(0.001)

    val json = moment.toJson

    val jsonStr = compact(render(json))
    println(jsonStr)

    val jsonRe = parse(jsonStr).asInstanceOf[JObject]

    SGD.fromJson(jsonRe)
  }

  test("TransFunc") {
    val identity = new Identity()
    val relu = new Relu()
    val softmax = new Softmax()
    val tanh = new Tanh()
    val sigmoid = new Sigmoid()

    val sigmoidWithDropout = new SigmoidWithDropout(0.5, "IncTrain")
    val tanhWithDropout = new TanhWithDropout(0.5, "IncTrain")
    val dropout = new Dropout(0.5, "IncTrain")

    val transFuncs = List(identity, relu, softmax, tanh, sigmoid, sigmoidWithDropout, tanhWithDropout, dropout)

    transFuncs.foreach( trans => println(compact(render(trans.toJson))))
    val jsonStrs = transFuncs.map( trans => compact(render(trans.toJson)))
    println()
    jsonStrs.foreach{ jsonStr =>
      val json = parse(jsonStr).asInstanceOf[JObject]
      println(json.obj.head)
      TransFunc.fromJson(json)
    }
  }

  test("LossFuncs") {
    val logLoss = new LogLoss()
    val l2Loss = new L2Loss()
    val huberLoss = new HuberLoss(0.5)
    val hingeLoss = new HingeLoss()
    val softmaxLoss = new SoftmaxLoss()
    val crossEntropyLoss = new CrossEntropyLoss()

    val transFuncs = List(logLoss, l2Loss, huberLoss, hingeLoss, crossEntropyLoss, softmaxLoss)

    transFuncs.foreach( trans => println(compact(render(trans.toJson))))
    val jsonStrs = transFuncs.map( trans => compact(render(trans.toJson)))
    println()
    jsonStrs.foreach{ jsonStr =>
      val json = parse(jsonStr).asInstanceOf[JObject]
      println(json.obj.head)
      LossFunc.fromJson(json)
    }
  }

  test("Layers") {
    val conf = SharedConf.get()
    implicit val graph: Graph = new LocalGraph(new PlaceHolder(), conf)
    val opt = new Adam(0.01, 0.9, 0.99)
    // 20: field, 10 output
    val ipLayer = new SimpleInputLayer("inputLayer", 10, new Identity, opt)
    val embedding = new Embedding("embedding", 20 * 8, 8, opt)
    val fc1 = new FCLayer("fc1", 200, embedding, new Relu, opt)
    val fc2 = new FCLayer("fc2", 200, fc1, new Relu, opt)
    val fc3 = new FCLayer("fc3", 10, fc2, new Identity, opt)
    val combine = new SumPooling("sum", 10, Array(ipLayer, fc3))
    val loss = new SimpleLossLayer("loss", combine, new SoftmaxLoss)

    // println(JsonUtils.layer2JsonPretty(loss))
    val jsonStr = """
      |{
      |  "fc3":{
      |    "type":"FCLayer",
      |    "outputdim":10,
      |    "inputlayer":"fc2",
      |    "transfunc":"Identity"
      |  },
      |  "fc2":{
      |    "type":"FCLayer",
      |    "outputdim":200,
      |    "inputlayer":"fc1",
      |    "optimizer":{
      |      "type":"Adam",
      |      "beta":0.9
      |    }
      |  },
      |  "embedding":{
      |    "type":"Embedding",
      |    "outputdim":160,
      |    "numfactors":8,
      |    "optimizer":{
      |      "type":"Adam",
      |      "beta":0.9,
      |      "gamma":0.99
      |    }
      |  },
      |  "inputLayer":{
      |    "type":"SimpleInputLayer",
      |    "outputdim":10
      |  },
      |  "sum":{
      |    "type":"SumPooling",
      |    "outputdim":10,
      |    "inputlayers":["inputLayer","fc3"]
      |  },
      |  "fc1":{
      |    "type":"FCLayer",
      |    "outputdim":200,
      |    "inputlayer":"embedding",
      |    "transfunc":{
      |      "type":"Relu"
      |    },
      |    "optimizer":{
      |      "type":"Adam",
      |      "gamma":0.99
      |    }
      |  },
      |  "loss":{
      |    "type":"SimpleLossLayer",
      |    "outputdim":-1,
      |    "lossfunc":"SoftmaxLoss",
      |    "inputlayer":"sum"
      |  }
      |}
    """.stripMargin

    JsonUtils.layerFromJson(parse(jsonStr).asInstanceOf[JObject])
    println(JsonUtils.layer2JsonPretty(graph.getLossLayer.asInstanceOf[Layer]))
  }

  test("ReadJson"){
    val conf = SharedConf.get()
    val json = "deepfm"
    val jsonPath = s"E:\\github\\fitzwang\\angel\\mlcore\\src\\test\\jsons\\$json.json"
    val layers = JsonUtils.parseAndUpdateJson(jsonPath, conf)

    implicit val graph: Graph = new LocalGraph(new PlaceHolder(), conf)
    JsonUtils.layerFromJson(layers)

    val topLayer = graph.getLossLayer
    println(JsonUtils.layer2JsonPretty(topLayer.asInstanceOf[Layer]))

    // println(pretty(render(layers)))
  }
}
