package com.tencent.angel.ml.treemodels.tree.regression;

import com.tencent.angel.ml.treemodels.param.RegTParam;
import com.tencent.angel.ml.treemodels.tree.basic.Tree;

public class RegTree extends Tree<RegTParam, RegTNode> {

    public RegTree(RegTParam param) {
        super(param);
        RegTNode root = new RegTNode(0, null, param.numClass);
        this.nodes.put(0, root);
    }
}
