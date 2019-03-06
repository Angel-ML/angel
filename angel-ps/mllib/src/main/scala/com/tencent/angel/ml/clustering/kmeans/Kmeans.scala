package com.tencent.angel.ml.clustering.kmeans

import com.tencent.angel.ml.core.PSOptimizerProvider
import com.tencent.angel.ml.core.conf.MLCoreConf
import com.tencent.angel.ml.core.graphsubmit.AngelModel
import com.tencent.angel.ml.core.network.Identity
import com.tencent.angel.ml.core.network.layers.LossLayer
import com.tencent.angel.ml.core.network.layers.verge.KmeansInputLayer
import com.tencent.angel.ml.core.optimizer.loss.KmeansLoss
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

class Kmeans(conf: Configuration, _ctx: TaskContext = null) extends AngelModel(conf, _ctx) {
  val optProvider = new PSOptimizerProvider()

  override def buildNetwork(): Unit = {
    val ipOptNmae = sharedConf.get(MLCoreConf.ML_INPUTLAYER_OPTIMIZER, MLCoreConf.DEFAULT_ML_INPUTLAYER_OPTIMIZER)
    val K = sharedConf.getInt(MLCoreConf.KMEANS_CENTER_NUM, MLCoreConf.DEFAULT_KMEANS_CENTER_NUM)
    val input = new KmeansInputLayer("input", K, new Identity(), optProvider.getOptimizer(ipOptNmae))

    new LossLayer("simpleLossLayer", input, new KmeansLoss())
  }
}
