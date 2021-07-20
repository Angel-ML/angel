package com.tencent.angel.spark.ml.featureEngineering.FillMissingValue

import com.google.gson.JsonParser

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by isakjiang on 2020-11-16
 */
object FillMissingValueConf {

  //the output content is feature ids, missingValue, fillMethod
  // here fillMethod include: mean、median、count
  def parser(featureConfName: String): Array[(String, String, String)] = {

    println("the actual conf name is: " + featureConfName)
    //读配置文件
    val jsonString = Source.fromFile(featureConfName, "utf-8").mkString
    //解析成json字符串格式
    val json = new JsonParser()
    val jsonObject = json.parse(jsonString).getAsJsonObject

    val featureJson = jsonObject.getAsJsonArray("feature")
    val resultConf = new ArrayBuffer[(String, String, String)]

    //sparse for featureJson
    if (featureJson != null) {
      val featureIter = featureJson.iterator()
      while (featureIter.hasNext) {
        val intemTmp = featureIter.next().getAsJsonObject
        val id = intemTmp.get("id").getAsString
        val fillMethod = if (intemTmp.get("fillMethod") != null) intemTmp.get("fillMethod").getAsString else null
        val missingValue = if (intemTmp.get("missingValue") != null) intemTmp.get("missingValue").getAsString else null

        resultConf += Tuple3(id, missingValue, fillMethod)
      }
    }

    resultConf.toArray
  }

}
