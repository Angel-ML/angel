package com.tencent.angel.spark.ml.tree.kdtree.tree;

import com.tencent.angel.spark.ml.tree.kdtree.histogram.SumPair;
import com.tencent.angel.spark.ml.tree.split.SplitEntry;
import java.io.Serializable;

public class KDTSplit implements Serializable {

  private SplitEntry splitEntry;
  private SumPair[] leftSumPair;
  private SumPair[] rightSumPair;

  public KDTSplit() {this(null, null, null);}

  public KDTSplit(SplitEntry splitEntry, SumPair[] left, SumPair[] right) {
    super();
    this.splitEntry = splitEntry;
    this.leftSumPair = left;
    this.rightSumPair = right;
  }

  public boolean isValid(float minSplitVariance) {
    return splitEntry != null && !splitEntry.isEmpty();
  }

  public boolean needReplace(KDTSplit split) {
    if (this.splitEntry != null) {
      return split.splitEntry != null && !this.splitEntry.needReplace(split.splitEntry);
    } else {
      return split.splitEntry != null;
    }
  }

  public void update(KDTSplit split) {
    if (this.needReplace(split)) {
      this.splitEntry = split.splitEntry;
      this.leftSumPair = split.leftSumPair;
      this.rightSumPair = split.rightSumPair;
    }
  }

  public SplitEntry getSplitEntry() {
    return splitEntry;
  }

  public void setSplitEntry(SplitEntry splitEntry) {
    this.splitEntry = splitEntry;
  }

  public SumPair[] getLeftSumPairs() {
    return leftSumPair;
  }

  public SumPair[] getRightSumPairs() {
    return rightSumPair;
  }

  public void setLeftSumPair(int fid, SumPair leftSumPair) {
    this.leftSumPair[fid] = leftSumPair;
  }

  public void setRightSumPair(int fid, SumPair rightSumPair) {
    this.rightSumPair[fid] = rightSumPair;
  }
}
