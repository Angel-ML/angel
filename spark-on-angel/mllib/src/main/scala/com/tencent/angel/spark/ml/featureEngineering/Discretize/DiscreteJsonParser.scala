package com.tencent.angel.spark.ml.featureEngineering.Discretize

import com.google.gson.JsonParser
import com.tencent.angel.spark.ml.util.LogUtils

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by isakjiang on 2020-11-16
 */
object DiscreteJsonParser {

  //根据配置文件名解析每个特征的配置
  def parser(featureConfName: String): Array[(Int, String, Int, String, String)] = {

    LogUtils.logTime("the actual conf name is: " + featureConfName)
    //读配置文件
    val jsonString = Source.fromFile(featureConfName, "utf-8").mkString
    //解析成json字符串格式
    val json = new JsonParser()
    val jsonObject = json.parse(jsonString).getAsJsonObject

    val featureJson = jsonObject.getAsJsonArray("feature")
    val resultConf = new ArrayBuffer[(Int, String, Int, String, String)]

    //sparse for featureJson
    if (featureJson != null) {
      val featureIter = featureJson.iterator()
      while (featureIter.hasNext) {
        val intemTmp = featureIter.next().getAsJsonObject
        val id = intemTmp.get("id").getAsInt
        val discreteType = intemTmp.get("discreteType").getAsString
        val numBin = intemTmp.get("numBin").getAsInt
        val min = if (intemTmp.get("min") != null) intemTmp.get("min").getAsString else null
        val max = if (intemTmp.get("max") != null) intemTmp.get("max").getAsString else null
        resultConf += Tuple5(id, discreteType, numBin, min, max)
      }
    }
    resultConf.toArray

  }
}
