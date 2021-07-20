package com.tencent.angel.spark.ml.featureEngineering.Scaler

import com.google.gson.JsonParser
import com.tencent.angel.spark.ml.util.DataLoader

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by isakjiang on 2020-11-16
 */
object ScalerJsonParser {
  //return: featureId,type,min,max,isStd,isMean
  def parserJson(featureConfName: String): Array[(Int, String, Double, Double, Boolean, Boolean, String, String)] = {
    println("the actual conf name is: " + featureConfName)

    val jsonString = Source.fromFile(featureConfName, "utf-8").mkString
    //解析成json字符串格式
    val json = new JsonParser()
    val jsonObject = json.parse(jsonString).getAsJsonObject

    val minmax = jsonObject.getAsJsonArray("minmax")
    val standard = jsonObject.getAsJsonArray("standard")


    //result feature conf for all
    val resultConf = new ArrayBuffer[(Int, String, Double, Double, Boolean, Boolean, String, String)]

    //sparse for minmax
    if (minmax != null) {
      val minmaxIter = minmax.iterator()
      while (minmaxIter.hasNext) {
        val intemTmp = minmaxIter.next().getAsJsonObject
        val min = intemTmp.get("min").getAsDouble
        val max = intemTmp.get("max").getAsDouble
        val featureCols = DataLoader.parseFeatureCols(intemTmp.get("colStr").getAsString)
          .flatMap { case (begin, end) => begin to end }
        featureCols.map(id => resultConf += Tuple8(id, "minmax", min, max, false, false, null, null))
      }
    }

    //sparse for standard
    if (standard != null) {
      val standardIter = standard.iterator()
      while (standardIter.hasNext) {
        val intemTmp = standardIter.next().getAsJsonObject
        val std = intemTmp.get("std").getAsBoolean
        val mean = intemTmp.get("mean").getAsBoolean

        val featureCols = DataLoader.parseFeatureCols(intemTmp.get("colStr").getAsString)
          .flatMap { case (begin, end) => begin to end }
        featureCols.map(id => resultConf += Tuple8(id, "standard", 0.0, 0.0, std, mean, null, null))

      }
    }
    resultConf.toArray
  }

}
