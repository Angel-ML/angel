package com.tencent.angel.spark.ml.tree.gbdt.tree;

import com.tencent.angel.spark.ml.tree.tree.basic.Tree;
import com.tencent.angel.spark.ml.tree.tree.param.GBDTParam;

public class GBTTree extends Tree<GBDTParam, GBTNode> {
    public GBTTree(GBDTParam param) {
        super(param);
        GBTNode root = new GBTNode(0, null, param.numClass);
        this.nodes.put(0, root);
    }
}
