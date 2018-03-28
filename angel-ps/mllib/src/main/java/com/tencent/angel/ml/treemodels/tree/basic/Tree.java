package com.tencent.angel.ml.treemodels.tree.basic;

import com.tencent.angel.ml.treemodels.param.TreeParam;

import java.util.HashMap;
import java.util.Map;

public abstract class Tree<TParam extends TreeParam, Node extends TNode> {
    protected final TParam param;
    private int[] fset; // features used in this tree, null means all the features are used
    protected Map<Integer, Node> nodes; // nodes in the tree

    public Tree(TParam param) {
        this.param = param;
        this.nodes = new HashMap<>();
    }

    public int[] getFset() {
        return this.fset;
    }

    public Node getRoot() {
        return this.nodes.get(0);
    }

    public Node getNode(int nid) {
        return this.nodes.get(nid);
    }

    public void setNode(int nid, Node node) {
        this.nodes.put(nid, node);
    }

    public void setFset(int[] fset) {
        this.fset = fset;
    }
}
