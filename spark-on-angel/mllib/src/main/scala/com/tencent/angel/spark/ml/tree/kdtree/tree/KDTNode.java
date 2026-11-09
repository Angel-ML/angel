package com.tencent.angel.spark.ml.tree.kdtree.tree;

import com.tencent.angel.spark.ml.tree.basic.TNode;
import com.tencent.angel.spark.ml.tree.kdtree.histogram.SumPair;

public class KDTNode extends TNode<KDTNodeStat> {

  private int fid;
  private float variance;
  private float splitValue;
  private SumPair[] sumPairs;

  public KDTNode(int nid, TNode parent, TNode left, TNode right) {
    super(nid, parent, left, right);
  }

  public KDTNode(int nid, TNode parent) {
    super(nid, parent);
  }

  public SumPair[] getSumPairs() { return sumPairs; }

  public void setSumPairs(SumPair[] sumPairs) {
    this.sumPairs = sumPairs; }

  public int getFid() {
    return fid;
  }

  public void setFid(int fid) {
    this.fid = fid;
  }

  public float getVariance() {
    return variance;
  }

  public void setVariance(float variance) {

    this.variance = variance;
  }

  public float getSplitValue() {
    return splitValue;
  }

  public void setSplitValue(float splitValue) {
    this.splitValue = splitValue;
  }
}
