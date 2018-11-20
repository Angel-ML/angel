package com.tencent.angel.ml.tree.model

import com.tencent.angel.ml.core.conf.MLConf
import com.tencent.angel.ml.tree.conf.Algo
import com.tencent.angel.ml.tree.conf.Algo.{Algo, Classification}
import com.tencent.angel.ml.tree.conf.EnsembleStrategy.{Average, Vote}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

object RandomForestModel {

  def apply(conf: Configuration): RandomForestModel = {
    RandomForestModel(conf, null)
  }

    def apply(conf: Configuration, _ctx: TaskContext): RandomForestModel = {
    val algo = conf.get(MLConf.ML_TREE_TASK_TYPE,
      MLConf.DEFAULT_ML_TREE_TASK_TYPE)
    val numTrees = conf.getInt(MLConf.ML_NUM_TREE,
      MLConf.DEFAULT_ML_NUM_TREE)
    RandomForestModel(Algo.fromString(algo), new Array[DecisionTreeModel](numTrees), conf, _ctx)
  }

  def apply(algo: Algo, trees: Array[DecisionTreeModel],
            conf: Configuration): RandomForestModel = {
    RandomForestModel(algo, trees, conf, _ctx =null)
  }

  def apply(algo: Algo, trees: Array[DecisionTreeModel], conf: Configuration, _ctx: TaskContext): RandomForestModel = {
    new RandomForestModel(algo, trees, conf, _ctx)
  }
}


/**
  * Represents a random forest model.
  *
  * @param algo algorithm for the ensemble model, either Classification or Regression
  * @param trees tree ensembles
  */
class RandomForestModel (
                          override val algo: Algo,
                          override val trees: Array[DecisionTreeModel],
                          conf: Configuration,
                          _ctx: TaskContext)
  extends TreeEnsembleModel(algo, trees, Array.fill(trees.length)(1.0),
    combiningStrategy = if (algo == Classification) Vote else Average, conf, _ctx) {

  require(trees.forall(_.algo == algo))
}
