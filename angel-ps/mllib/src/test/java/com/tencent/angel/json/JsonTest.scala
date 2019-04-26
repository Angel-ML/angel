package com.tencent.angel.json

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.layers.{AngelGraph, PlaceHolder}
import com.tencent.angel.ml.core.utils.paramsutils.JsonUtils
import org.json4s._
import org.json4s.jackson.JsonMethods.{parse, pretty, render}
import org.scalatest.FunSuite

class JsonTest extends FunSuite{
  test("123") {
    val sharedConf = SharedConf.get()
    val placeHolder = new PlaceHolder(sharedConf)
    implicit val graph: AngelGraph = new AngelGraph(placeHolder, sharedConf)
    val jsonFile = "angel-ps\\mllib\\src\\test\\jsons\\graph.json"
    val jsonAst = JsonUtils.parseJson(jsonFile)
    JsonUtils.fillGraph(jsonAst)

    val json = graph.toJson

    println(pretty(render(json)))
  }
}
