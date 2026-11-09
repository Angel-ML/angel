package com.tencent.angel.spark.ml.util

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.{ParamPair, Params}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JObject, JValue, _}

// This originates from apache-spark DefaultPramsWriter copy paste
object DefaultAngelParamsWriter {

  /**
   * Saves metadata + Params to: path + "/metadata"
   *  - class
   *  - timestamp
   *  - sparkVersion
   *  - uid
   *  - paramMap
   *  - (optionally, extra metadata)
   *
   * @param extraMetadata Extra metadata to be saved at same level as uid, paramMap, etc.
   * @param paramMap      If given, this is saved in the "paramMap" field.
   *                      Otherwise, all [[org.apache.spark.ml.param.Param]]s are encoded using
   *                      [[org.apache.spark.ml.param.Param.jsonEncode()]].
   */
  def saveMetadata(
                    instance: Params,
                    path: String,
                    sc: SparkContext,
                    extraMetadata: Option[JObject] = None,
                    paramMap: Option[JValue] = None): Unit = {

    val metadataPath = new Path(path, "metadata").toString
    val metadataJson = getMetadataToSave(instance, sc, extraMetadata, paramMap)
    sc.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataPath)
  }

  /**
   * Helper for [[saveMetadata()]] which extracts the JSON to save.
   * This is useful for ensemble models which need to save metadata for many sub-models.
   *
   * @see [[saveMetadata()]] for details on what this includes.
   */
  def getMetadataToSave(
                         instance: Params,
                         sc: SparkContext,
                         extraMetadata: Option[JObject] = None,
                         paramMap: Option[JValue] = None): String = {
    val uid = instance.uid
    val cls = instance.getClass.getName
    val params = instance.extractParamMap().toSeq.asInstanceOf[Seq[ParamPair[Any]]]
    val jsonParams = paramMap.getOrElse(render(params.filter {
      case ParamPair(p, _) => p != null
    }.map {
      case ParamPair(p, v) =>
        p.name -> parse(p.jsonEncode(v))
    }.toList))
    val basicMetadata = ("class" -> cls) ~
      ("timestamp" -> System.currentTimeMillis()) ~
      ("sparkVersion" -> sc.version) ~
      ("uid" -> uid) ~
      ("paramMap" -> jsonParams)
    val metadata = extraMetadata match {
      case Some(jObject) =>
        basicMetadata ~ jObject
      case None =>
        basicMetadata
    }
    val metadataJson: String = compact(render(metadata))
    metadataJson
  }
}
