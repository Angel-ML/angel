package com.tencent.angel.ml.treemodels.tree.basic;

public abstract class TNode<NodeStat extends TNodeStat> {
    private final int nid;  // node id in the tree
    private final TNode parent;  // node id of parent in the tree, start with 0
    private TNode leftChild; // node id of left child in the tree
    private TNode rightChild; // node id of right child in the tree
    private SplitEntry splitEntry;
    private boolean isLeaf;

    protected NodeStat[] nodeStats;  // stats of current node, each stats stands for a class

    public TNode(int nid, TNode parent, TNode left, TNode right) {
        this.nid = nid;
        this.parent = parent;
        this.leftChild = left;
        this.rightChild = right;
        this.isLeaf = false;
    }

    public TNode(int nid, TNode parent) {
        this(nid, parent, null, null);
    }

    public int getNid() {
        return this.nid;
    }

    public TNode getParent() {
        return this.parent;
    }

    public TNode getLeftChild() {
        return this.leftChild;
    }

    public TNode getRightChild() {
        return this.rightChild;
    }

    public SplitEntry getSplitEntry() {
        return splitEntry;
    }

    public float getGain() {
        return nodeStats[0].getGain();
    }

    public float[] getGains() {
        float[] gains = new float[nodeStats.length];
        for (int i = 0; i < nodeStats.length; i++) {
            gains[i] = nodeStats[i].getGain();
        }
        return gains;
    }

    public NodeStat getNodeStat() {
        return this.nodeStats[0];
    }

    public NodeStat getNodeStat(int classId) {
        return nodeStats[classId];
    }

    public NodeStat[] getNodeStats() {
        return this.nodeStats;
    }

    public void setLeftChild(TNode leftChild) {
        this.leftChild = leftChild;
    }

    public void setRightChild(TNode rightChild) {
        this.rightChild = rightChild;
    }

    public void setSplitEntry(SplitEntry splitEntry) {
        this.splitEntry = splitEntry;
    }

    public void setGain(float gain) {
        this.nodeStats[0].setGain(gain);
    }

    public void setGains(float[] gains) {
        for (int i = 0; i < this.nodeStats.length; i++) {
            this.nodeStats[i].setGain(gains[i]);
        }
    }

    public void setNodeWeight(float nodeWeight) {
        this.nodeStats[0].setNodeWeight(nodeWeight);
    }

    public void setNodeWeights(float[] nodeWeights) {
        for (int i = 0; i < this.nodeStats.length; i++) {
            this.nodeStats[i].setNodeWeight(nodeWeights[i]);
        }
    }

    public boolean isLeaf() {
        return this.isLeaf;
    }

    public void chgToLeaf() {
        this.isLeaf = true;
    }

}
