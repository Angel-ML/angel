package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.Model
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.layers.PlaceHolder
import com.tencent.angel.ml.core.utils.JsonUtils


class LocalModel(conf: SharedConf) extends Model {
  override implicit val graph: Graph = new LocalGraph(new PlaceHolder(conf), conf)

  override def buildNetwork(): Unit = {
    JsonUtils.layerFromJson(conf.getJson)
  }
}
