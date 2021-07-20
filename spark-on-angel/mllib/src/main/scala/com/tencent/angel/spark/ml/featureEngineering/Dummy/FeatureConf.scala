package com.tencent.angel.spark.ml.featureEngineering.Dummy

/**
 * Created by isakjiang on 2020-11-16
 */

import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source


class FeatureConf(confPath: String) {

  // TODO: extract value with case class
  case class Fields(name: String, index: Int)

  case class IdFeature(name: String)

  case class CombFeature(name: String, dependencies: String)

  case class FeatureCross(id_feature: Option[List[IdFeature]], comb_feature: Option[List[CombFeature]])

  case class WholeConf(fields: List[Fields], feature_cross: FeatureCross)

  var field2IndexMap: Map[String, Int] = _
  var idFeatures: Array[(String, Int)] = _
  var combFeature: Array[(String, Array[Int])] = _

  def parseConfig: this.type = {
    implicit val formats = DefaultFormats
    val sc = SparkContext.getOrCreate()
    val confString = Source.fromFile(confPath, "utf-8").mkString
    val json = parse(StringInput(confString))

    field2IndexMap = (json \ "fields").extract[List[Map[String, String]]]
      .map { dict => (dict("name"), dict("index").toInt) }.toMap

    val crossJson = json \ "feature_cross"

    idFeatures = (crossJson \ "id_features").extract[List[Map[String, String]]]
      .map { dict => (dict("name"), field2IndexMap(dict("name"))) }.toArray

    combFeature = (crossJson \ "comb_features").extract[List[Map[String, String]]]
      .map { dict =>
        val name = dict("name")
        val dependencies = dict("dependencies").split(",").map { x => field2IndexMap(x) }
        (name, dependencies)
      }.toArray

    this
  }
}

object FeatureConf {
  def apply(confPath: String): FeatureConf = {
    new FeatureConf(confPath).parseConfig
  }
}
