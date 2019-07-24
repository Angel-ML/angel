package com.tencent.angel.ml.core.utils

import java.io.{BufferedReader, FileInputStream, IOException, InputStreamReader}
import java.net.URI
import java.util

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.layers.multiary._
import com.tencent.angel.ml.core.network.layers.unary._
import com.tencent.angel.ml.core.network.layers.leaf._
import com.tencent.angel.ml.core.network.{Graph, TransFunc}
import com.tencent.angel.ml.core.optimizer.{Optimizer, OptimizerProvider}
import com.tencent.angel.ml.core.optimizer.loss.LossFunc
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.ParserUtil.ParseException
import org.json4s.native.JsonMethods._

import scala.collection.mutable
import scala.reflect.ClassTag

object LayerKeys {
  val typeKey: String = "type"
  val outputDimKey: String = "outputdim"
  val outputDimsKey: String = "outputdims"
  val inputLayerKey: String = "inputlayer"
  val inputLayersKey: String = "inputlayers"
  val numFactorsKey: String = "numfactors"
  val optimizerKey: String = "optimizer"
  val transFuncKey: String = "transfunc"
  val transFuncsKey: String = "transfuncs"
  val lossFuncKey: String = "lossfunc"
  val weightDimKey: String = "weightdim"
  val weightDimsKey: String = "weightdims"
}

object TransFuncKeys {
  val typeKey: String = "type"
  val probaKey: String = "proba"
  val actionTypeKey: String = "actiontype"
}

object LossFuncKeys {
  val typeKey: String = "type"
  val deltaKey: String = "delta"
}

object JsonTopKeys {
  val data: String = "data"
  val model: String = "model"
  val train: String = "train"
  val default_optimizer: String = "default_optimizer"
  val default_trandfunc: String = "default_trandfunc"
  val layers: String = "layers"
}

object OptimizerKeys {
  val typeKey: String = "type"
  val alphaKey: String = "alpha"
  val betaKey: String = "beta"
  val gammaKey: String = " gamma"
  val momentumKey: String = " momentum"
  val reg1Key: String = "reg1"
  val reg2Key: String = "reg2"
}

object JsonUtils {
  private implicit val formats: DefaultFormats.type = DefaultFormats

  def extract[T: Manifest](jast: JValue, key: String, default: Option[T] = None): Option[T] = {
    jast \ key match {
      case JNothing => default
      case value => Some(value.extract[T](formats, implicitly[Manifest[T]]))
    }
  }

  def matchClassName[T: ClassTag](name: String): Boolean = {
    val runtimeClassName = implicitly[ClassTag[T]].runtimeClass.getSimpleName

    runtimeClassName.equalsIgnoreCase(name)
  }

  def fieldEqualClassName[T: ClassTag](obj: JObject, fieldName: String = "type"): Boolean = {
    val runtimeClassName = implicitly[ClassTag[T]].runtimeClass.getSimpleName

    val name = extract[String](obj, fieldName)
    if (name.isEmpty) {
      false
    } else {
      runtimeClassName.equalsIgnoreCase(name.get)
    }
  }

  def J2Pretty(json: JValue): String = pretty(render(json))

  def json2String(obj: JValue): String = {
    obj \ LayerKeys.optimizerKey match {
      case JString(opt) => opt
      case opt: JObject => J2Pretty(opt)
      case _ => "Momentum"
    }
  }

  def string2Json(jsonstr: String): JValue = {
    try {
      parse(jsonstr)
    } catch {
      case _: ParseException =>
        if (jsonstr.startsWith("\"")) {
          JString(jsonstr.substring(1, jsonstr.length - 1))
        } else {
          JString(jsonstr)
        }
      case e: Exception => JNothing
    }
  }

  def layer2Json(topLayer: Layer)(implicit jMap: mutable.HashMap[String, JField]): Unit = {
    topLayer match {
      case l: InputLayer =>
        if (!jMap.contains(l.name)) {
          jMap.put(l.name, l.toJson)
        }
      case l: LinearLayer =>
        if (!jMap.contains(l.name)) {
          jMap.put(l.name, l.toJson)
        }
        layer2Json(l.inputLayer)(jMap)
      case l: JoinLayer =>
        if (!jMap.contains(l.name)) {
          jMap.put(l.name, l.toJson)
        }
        l.inputLayers.foreach(layer => layer2Json(layer)(jMap))
      case l: LossLayer =>
        if (!jMap.contains(l.name)) {
          jMap.put(l.name, l.toJson)
        }
    }
  }

  def layer2JsonPretty(topLayer: Layer): String = {
    implicit val jsonMap: mutable.HashMap[String, JField] = new mutable.HashMap[String, JField]()
    layer2Json(topLayer: Layer)
    J2Pretty(JObject(jsonMap.values.toList))
  }

  def layerFromJson(implicit graph: Graph): Unit = {
    layerFromJson(graph.conf.getJson)
  }

  def layerFromJson(jast: JObject)(implicit graph: Graph): Unit = {
    val optimizerProvider: OptimizerProvider = Optimizer.getOptimizerProvider(graph.conf.optJsonProvider(), graph.conf)

    val layerMap = new mutable.HashMap[String, Layer]()
    val fieldList = new util.ArrayList[JField]()
    jast.obj.foreach(field => fieldList.add(field))

    while (layerMap.size < jast.obj.size) {
      val iter = fieldList.iterator()
      while (iter.hasNext) {
        val field = iter.next()
        val name = field._1
        val obj = field._2

        (obj \ LayerKeys.typeKey) match {
          case JString(value) if matchClassName[ConcatLayer](value) =>
            val inputLayers = extract[Array[String]](obj, LayerKeys.inputLayersKey)
            if (inputLayers.nonEmpty && inputLayers.get.forall(layer => layerMap.contains(layer))) {
              val newLayer = new ConcatLayer(name,
                extract[Int](obj, LayerKeys.outputDimKey).get,
                inputLayers.get.map(layer => layerMap(layer))
              )
              layerMap.put(name, newLayer)
              iter.remove()
            }
          case JString(value) if matchClassName[DotPooling](value) =>
            val inputLayers = extract[Array[String]](obj, LayerKeys.inputLayersKey)
            if (inputLayers.nonEmpty && inputLayers.get.forall(layer => layerMap.contains(layer))) {
              val newLayer = new DotPooling(name,
                extract[Int](obj, LayerKeys.outputDimKey).get,
                inputLayers.get.map(layer => layerMap(layer))
              )
              layerMap.put(name, newLayer)
              iter.remove()
            }
          case JString(value) if matchClassName[SumPooling](value) =>
            val inputLayers = extract[Array[String]](obj, LayerKeys.inputLayersKey)
            if (inputLayers.nonEmpty && inputLayers.get.forall(layer => layerMap.contains(layer))) {
              val newLayer = new SumPooling(name,
                extract[Int](obj, LayerKeys.outputDimKey).get,
                inputLayers.get.map(layer => layerMap(layer))
              )
              layerMap.put(name, newLayer)
              iter.remove()
            }
          case JString(value) if matchClassName[MulPooling](value) =>
            val inputLayers = extract[Array[String]](obj, LayerKeys.inputLayersKey)
            if (inputLayers.nonEmpty && inputLayers.get.forall(layer => layerMap.contains(layer))) {
              val newLayer = new MulPooling(name,
                extract[Int](obj, LayerKeys.outputDimKey).get,
                inputLayers.get.map(layer => layerMap(layer))
              )
              layerMap.put(name, newLayer)
              iter.remove()
            }
          case JString(value) if matchClassName[Embedding](value) =>
            val newLayer = new Embedding(name,
              extract[Int](obj, LayerKeys.outputDimKey).get,
              extract[Int](obj, LayerKeys.numFactorsKey).get,
              optimizerProvider.optFromJson(getOptimizerString(obj))
            )

            layerMap.put(name, newLayer)
            iter.remove()
          case JString(value) if matchClassName[SimpleInputLayer](value) =>
            val newLayer = new SimpleInputLayer(name,
              extract[Int](obj, LayerKeys.outputDimKey).get,
              TransFunc.fromJson(obj \ LayerKeys.transFuncKey),
              optimizerProvider.optFromJson(getOptimizerString(obj))
            )

            layerMap.put(name, newLayer)
            iter.remove()
          case JString(value) if matchClassName[LossLayer](value) || value.equalsIgnoreCase("SimpleLossLayer") =>
            val inputLayer = extract[String](obj, LayerKeys.inputLayerKey)
            if (inputLayer.nonEmpty && layerMap.contains(inputLayer.get)) {
              val newLayer = new LossLayer(name,
                layerMap(inputLayer.get),
                LossFunc.fromJson(obj \ LayerKeys.lossFuncKey)
              )

              layerMap.put(name, newLayer)
              iter.remove()
            }
          case JString(value) if matchClassName[BiInnerSumCross](value) =>
            val inputLayer = extract[String](obj, LayerKeys.inputLayerKey)
            if (inputLayer.nonEmpty && layerMap.contains(inputLayer.get)) {
              val newLayer = new BiInnerSumCross(name,
                layerMap(inputLayer.get)
              )

              layerMap.put(name, newLayer)
              iter.remove()
            }
          case JString(value) if matchClassName[FCLayer](value) =>
            val inputLayer = extract[String](obj, LayerKeys.inputLayerKey)
            if (inputLayer.nonEmpty && layerMap.contains(inputLayer.get)) {
              val newLayer = new FCLayer(name,
                extract[Int](obj, LayerKeys.outputDimKey).get,
                layerMap(inputLayer.get),
                TransFunc.fromJson(obj \ LayerKeys.transFuncKey),
                optimizerProvider.optFromJson(getOptimizerString(obj))
              )

              layerMap.put(name, newLayer)
              iter.remove()
            }
          case JString(value) if matchClassName[ParamSharedFC](value) =>
            val inputLayer = extract[String](obj, LayerKeys.inputLayerKey)
            if (inputLayer.nonEmpty && layerMap.contains(inputLayer.get)) {
              val newLayer = new ParamSharedFC(name,
                extract[Int](obj, LayerKeys.outputDimKey).get,
                layerMap(inputLayer.get),
                TransFunc.fromJson(obj \ LayerKeys.transFuncKey),
                extract[Int](obj, LayerKeys.weightDimKey).get,
                optimizerProvider.optFromJson(getOptimizerString(obj))
              )

              layerMap.put(name, newLayer)
              iter.remove()
            }
          case JString(value) if matchClassName[BiInnerCross](value) =>
            val inputLayer = extract[String](obj, LayerKeys.inputLayerKey)
            if (inputLayer.nonEmpty && layerMap.contains(inputLayer.get)) {
              val newLayer = new BiInnerCross(name,
                extract[Int](obj, LayerKeys.outputDimKey).get,
                layerMap(inputLayer.get)
              )

              layerMap.put(name, newLayer)
              iter.remove()
            }
          case JString(value) if matchClassName[BiInteractionCross](value) =>
            val inputLayer = extract[String](obj, LayerKeys.inputLayerKey)
            if (inputLayer.nonEmpty && layerMap.contains(inputLayer.get)) {
              val newLayer = new BiInteractionCross(name,
                extract[Int](obj, LayerKeys.outputDimKey).get,
                layerMap(inputLayer.get)
              )

              layerMap.put(name, newLayer)
              iter.remove()
            }
          case JString(value) if matchClassName[BiInteractionCrossTiled](value) =>
            val inputLayer = extract[String](obj, LayerKeys.inputLayerKey)
            if (inputLayer.nonEmpty && layerMap.contains(inputLayer.get)) {
              val newLayer = new BiInteractionCrossTiled(name,
                extract[Int](obj, LayerKeys.outputDimKey).get,
                layerMap(inputLayer.get)
              )

              layerMap.put(name, newLayer)
              iter.remove()
            }
          case JString(value) if matchClassName[WeightedSumLayer](value) =>
            val inputLayers = extract[Array[String]](obj, LayerKeys.inputLayersKey)
            if (inputLayers.nonEmpty && inputLayers.get.forall(layer => layerMap.contains(layer))) {
              val newLayer = new WeightedSumLayer(name,
                extract[Int](obj, LayerKeys.outputDimKey).get,
                inputLayers.get.map(layer => layerMap(layer))
              )
              layerMap.put(name, newLayer)
              iter.remove()
            }
          case JString(value) if matchClassName[CrossLayer](value) =>
            val inputLayers = extract[Array[String]](obj, LayerKeys.inputLayersKey)
            if (inputLayers.nonEmpty && inputLayers.get.forall(layer => layerMap.contains(layer))) {
              val newLayer = new CrossLayer(name,
                extract[Int](obj, LayerKeys.outputDimKey).get,
                inputLayers.get.map(layer => layerMap(layer)),
                optimizerProvider.optFromJson(getOptimizerString(obj))
              )
              layerMap.put(name, newLayer)
              iter.remove()
            }
        }
      }
    }
  }

  // for compatible purpose -----------------------------------------------------------------------------------------
  private def jArray2JObject(jArray: JArray, default_trans: Option[JValue], default_optimizer: Option[JValue])(implicit conf: SharedConf): JObject = {
    val fields = jArray.arr.flatMap {
      case obj: JObject if fieldEqualClassName[ParamSharedFC](obj) =>
        extendParamSharedFC(obj, default_trans, default_optimizer)
      case obj: JObject if fieldEqualClassName[FCLayer](obj) =>
        extendFCLayer(obj, default_trans, default_optimizer)
      case obj: JObject if fieldEqualClassName[SimpleInputLayer](obj) =>
        extendSimpleInputLayer(obj, default_trans, default_optimizer)
      case obj: JObject if fieldEqualClassName[Embedding](obj) =>
        extendEmbeddingLayer(obj, default_trans, default_optimizer)
      case obj: JObject =>
        extendLayer(obj, default_trans, default_optimizer)
    }

    JObject(fields)
  }

  private def extendFCLayer(obj: JObject, default_trans: Option[JValue], default_optimizer: Option[JValue])(implicit conf: SharedConf): List[JField] = {
    val name = (obj \ "name").asInstanceOf[JString].values
    val lType = obj \ LayerKeys.typeKey
    var inputLayer = obj \ LayerKeys.inputLayerKey
    val outputDims = (obj \ LayerKeys.outputDimsKey).asInstanceOf[JArray].arr

    val transFuncs = (obj \ LayerKeys.transFuncsKey) match {
      case JNothing => outputDims.indices.toList.map(_ => default_trans.getOrElse(TransFunc.defaultJson))
      case arr: JArray => arr.arr
      case _ => throw MLException("Json format error!")
    }

    assert(outputDims.size == transFuncs.size)

    val optimizer = (obj \ LayerKeys.optimizerKey) match {
      case JNothing => default_optimizer.getOrElse(defaultOptJson(conf))
      case opt: JObject => opt
      case opt: JString => opt
      case _ => throw MLException("Json format error!")
    }

    var i: Int = 0

    outputDims.zip(transFuncs).map {
      case (outputDim: JInt, transFunc: JValue) =>
        val newName: String = if (i + 1 == outputDims.size) {
          name
        } else {
          s"${name}_$i"
        }

        i += 1

        val field = JField(newName, (LayerKeys.typeKey -> lType) ~
          (LayerKeys.inputLayerKey -> inputLayer) ~
          (LayerKeys.outputDimKey -> outputDim) ~
          (LayerKeys.transFuncKey -> transFunc) ~
          (LayerKeys.optimizerKey -> optimizer)
        )

        inputLayer = JString(newName)
        field
      case _ => throw MLException("FCLayer Json error!")
    }

  }

  private def extendParamSharedFC(obj: JObject, default_trans: Option[JValue], default_optimizer: Option[JValue])(implicit conf: SharedConf): List[JField] = {
    val name = (obj \ "name").asInstanceOf[JString].values
    val lType = obj \ LayerKeys.typeKey
    var inputLayer = obj \ LayerKeys.inputLayerKey
    val outputDims = (obj \ LayerKeys.outputDimsKey).asInstanceOf[JArray].arr
    val weightDims = (obj \ LayerKeys.weightDimsKey).asInstanceOf[JArray].arr

    val transFuncs = (obj \ LayerKeys.transFuncsKey) match {
      case JNothing => outputDims.indices.toList.map(_ => default_trans.getOrElse(TransFunc.defaultJson))
      case arr: JArray => arr.arr
      case _ => throw MLException("Json format error!")
    }

    assert(outputDims.size == transFuncs.size)
    assert(weightDims.size == transFuncs.size)

    val optimizer = (obj \ LayerKeys.optimizerKey) match {
      case JNothing => default_optimizer.getOrElse(defaultOptJson(conf))
      case opt: JObject => opt
      case opt: JString => opt
      case _ => throw MLException("Json format error!")
    }

    var i: Int = 0

    val zipped = (outputDims zip transFuncs zip weightDims) map { case ((a, b), c) => (a, b, c)}
    zipped.map {
      case (outputDim: JInt, transFunc: JValue, weightDim: JValue) =>
        val newName: String = if (i + 1 == outputDims.size) {
          name
        } else {
          s"${name}_$i"
        }

        i += 1

        val field = JField(newName, (LayerKeys.typeKey -> lType) ~
          (LayerKeys.inputLayerKey -> inputLayer) ~
          (LayerKeys.outputDimKey -> outputDim) ~
          (LayerKeys.transFuncKey -> transFunc) ~
          (LayerKeys.weightDimKey -> weightDim) ~
          (LayerKeys.optimizerKey -> optimizer)
        )

        inputLayer = JString(newName)
        field
      case _ => throw MLException("ParamSharedFC Json error!")
    }
  }

  private def extendSimpleInputLayer(obj: JObject, default_trans: Option[JValue], default_optimizer: Option[JValue])(implicit conf: SharedConf): List[JField] = {
    val name = (obj \ "name").asInstanceOf[JString].values
    val addOpt = (obj \ LayerKeys.optimizerKey) match {
      case JNothing => obj ~ (LayerKeys.optimizerKey, default_optimizer.getOrElse(defaultOptJson(conf)))
      case _ => obj
    }
    val addTrans = (obj \ LayerKeys.transFuncKey) match {
      case JNothing => addOpt ~ (LayerKeys.transFuncKey, default_trans.getOrElse(TransFunc.defaultJson))
      case _ => addOpt
    }

    List(JField(name, addTrans))
  }

  private def extendEmbeddingLayer(obj: JObject, default_trans: Option[JValue], default_optimizer: Option[JValue])(implicit conf: SharedConf): List[JField] = {
    val name = (obj \ "name").asInstanceOf[JString].values
    val addOpt = (obj \ LayerKeys.optimizerKey) match {
      case JNothing => obj ~ (LayerKeys.optimizerKey, default_optimizer.getOrElse(defaultOptJson(conf)))
      case _ => obj
    }

    List(JField(name, addOpt))
  }

  private def extendLayer(obj: JObject, default_trans: Option[JValue], default_optimizer: Option[JValue]): List[JField] = {
    val name = (obj \ "name").asInstanceOf[JString].values

    List(JField(name, obj))
  }

  private def defaultOptJson(conf: SharedConf): JObject = {
    val momentum: Double = conf.getDouble(MLCoreConf.ML_OPT_MOMENTUM_MOMENTUM,
      MLCoreConf.DEFAULT_ML_OPT_MOMENTUM_MOMENTUM)

    (OptimizerKeys.typeKey -> JString("Momentum")) ~ (OptimizerKeys.momentumKey -> JDouble(momentum))
  }

  private def getOptimizerString(obj: JValue): String = {
    obj \ LayerKeys.optimizerKey match {
      case JString(opt) => opt
      case opt: JObject => J2Pretty(opt)
      case _ => "Momentum"
    }
  }
  //-----------------------------------------------------------------------------------------------------------------

  def parseAndUpdateJson(jsonFileName: String, conf: SharedConf, hadoopConf: Configuration): Unit = {
    val sb = new mutable.StringBuilder()
    if (jsonFileName.startsWith("hdfs://")) {
      println("jsonFileName is " + jsonFileName)
      var inputStream: FSDataInputStream = null
      var bufferedReader: BufferedReader = null
      try {
        inputStream = HDFSUtil.getFSDataInputStream(jsonFileName, hadoopConf)
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
        var lineTxt: String = bufferedReader.readLine()
        while (lineTxt != null) {
          sb.append(lineTxt.trim)
          lineTxt = bufferedReader.readLine()
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (bufferedReader != null) {
          bufferedReader.close()
        }
        if (inputStream != null) {
          HDFSUtil.close
        }
      }
    } else {
      val reader = new BufferedReader(new InputStreamReader(new FileInputStream(jsonFileName)))

      var line = reader.readLine()
      while (line != null) {
        sb.append(line)
        line = reader.readLine()
      }

      reader.close()
    }
    val jsonStr = sb.toString()

    val jast = parse(jsonStr).asInstanceOf[JObject]

    // println(pretty(render(jast)))

    (jast \ JsonTopKeys.data) match {
      case JNothing =>
      case obj: JObject => DataParams(obj).updateConf(conf)
      case _ => throw MLException("Json format error!")
    }

    (jast \ JsonTopKeys.model) match {
      case JNothing =>
      case obj: JObject => ModelParams(obj).updateConf(conf)
      case _ => throw MLException("Json format error!")
    }

    (jast \ JsonTopKeys.train) match {
      case JNothing =>
      case obj: JObject => TrainParams(obj).updateConf(conf)
      case _ => throw MLException("Json format error!")
    }

    val default_optimizer = (jast \ JsonTopKeys.default_optimizer) match {
      case JNothing => None
      case obj: JValue => Some(obj)
      case _ => throw MLException("Json format error!")
    }

    val default_trandfunc = (jast \ JsonTopKeys.default_trandfunc) match {
      case JNothing => None
      case obj: JValue => Some(obj)
      case _ => throw MLException("Json format error!")
    }

    val json = (jast \ JsonTopKeys.layers) match {
      case arr: JArray => jArray2JObject(arr, default_trandfunc, default_optimizer)(conf)
      case obj: JObject => obj
      case _ => throw MLException("Json format error!")
    }

    conf.setJson(json)
  }
}

object HDFSUtil {
  var fs: FileSystem = null
  var hdfsInStream: FSDataInputStream = null

  def getFSDataInputStream(path: String, conf: Configuration): FSDataInputStream = {
    try {
      fs = FileSystem.get(URI.create(path), conf)
      hdfsInStream = fs.open(new Path(path))
    } catch {
      case e: IOException => {
        e.printStackTrace()
      }
    }
    hdfsInStream
  }

  def close {
    try {
      if (hdfsInStream != null) {
        hdfsInStream.close
      }
      /*
      if (fs != null) {
        fs.close()
      }
      */
    }
    catch {
      case e: IOException => {
        e.printStackTrace()
      }
    }
  }
}


