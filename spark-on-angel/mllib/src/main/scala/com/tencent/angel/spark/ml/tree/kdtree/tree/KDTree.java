package com.tencent.angel.spark.ml.tree.kdtree.tree;

import com.tencent.angel.spark.ml.tree.basic.Tree;
import com.tencent.angel.spark.ml.tree.param.KDTParam;

public class KDTree extends Tree<KDTParam, KDTNode> {

  public KDTree(KDTParam param) {
    super(param);
    KDTNode root = new KDTNode(0, null);
    this.nodes.put(0, root);
  }
}
