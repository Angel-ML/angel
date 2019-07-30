package com.tencent.angel.ml.clustering.kmeans

import com.tencent.angel.ml.core.PSOptimizerProvider
import com.tencent.angel.mlcore.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.graphsubmit.AngelModel
import com.tencent.angel.mlcore.network.Identity
import com.tencent.angel.mlcore.network.layers.LossLayer
import com.tencent.angel.mlcore.network.layers.unary.KmeansInputLayer
import com.tencent.angel.mlcore.optimizer.loss.KmeansLoss
import com.tencent.angel.worker.task.TaskContext

 class Kmeans(conf: SharedConf, _ctx: TaskContext = null) extends AngelModel(conf, _ctx.getTotalTaskNum) {
  val optProvider = new PSOptimizerProvider(conf)

   override def buildNetwork(): this.type = {
    val ipOptNmae = conf.get(MLCoreConf.ML_INPUTLAYER_OPTIMIZER, MLCoreConf.DEFAULT_ML_INPUTLAYER_OPTIMIZER)
    val K = conf.getInt(MLCoreConf.KMEANS_CENTER_NUM, MLCoreConf.DEFAULT_KMEANS_CENTER_NUM)
    val input = new KmeansInputLayer("input", K, new Identity(), optProvider.getOptimizer(ipOptNmae))

     new LossLayer("simpleLossLayer", input, new KmeansLoss())

    this
  }
}
