package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.GraphModel
import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.layers.PlaceHolder
import com.tencent.angel.ml.core.utils.JsonUtils


class LocalModel(conf: SharedConf) extends GraphModel {
  override implicit val graph: Graph = new LocalGraph(new PlaceHolder(conf), conf, 1)

  override def buildNetwork(): Unit = {
    JsonUtils.layerFromJson(conf.getJson)
  }
}
