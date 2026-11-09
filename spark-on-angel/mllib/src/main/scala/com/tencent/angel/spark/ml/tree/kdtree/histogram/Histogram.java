package com.tencent.angel.spark.ml.tree.kdtree.histogram;

import java.io.Serializable;
import java.util.Arrays;

public class Histogram implements Serializable {

  private int[] cnts; // num in feature i accumulated by numBin
  private int numBin; // the number of splits of feature i
  private int startBinIdx; // start bin index
  private int endBinIdx; // end bin index
  private int flowFlag = -1; // data flow to left(0) or right(1), -1 represent root
  private boolean splitFlag = false; //


  public Histogram(int numBin) {
    this.cnts = new int[numBin];
    this.numBin = numBin;
  }

  public void accumulate(int index, int cnt) {
    cnts[index] += cnt;
  }

  /*
   * accumulate values, such as values[i] = values[0] + ... + values[i - 1]
   * and according to whether the parent node has been split, determine the split interval of the child node
   */
  public void accumulateByBin(int nid, Histogram parent) {
    for (int i = 1; i < cnts.length; i++) {
      cnts[i] += cnts[i - 1];
    }

    if (nid == 0) {
      flowFlag = -1;
      startBinIdx = 0;
      endBinIdx = cnts.length - 1;
    } else if (nid % 2 == 1) { // left child
      // if parent node has been split in some feature, the split interval of the child node halved
      if (parent.splitFlag) {
        startBinIdx = parent.startBinIdx;
        int j = startBinIdx;
        int count = 0;
        if (startBinIdx == 0 && parent.endBinIdx > parent.startBinIdx) {
          count = parent.cnts[parent.endBinIdx];
          while(j < parent.endBinIdx && parent.cnts[j] < count / 2) j += 1;
        } else {
          if (parent.endBinIdx > parent.startBinIdx) {
            count = parent.cnts[parent.endBinIdx] - parent.cnts[parent.startBinIdx - 1];
            while(j < parent.endBinIdx && (parent.cnts[j] - parent.cnts[parent.startBinIdx - 1] ) < count / 2)
              j += 1;
          } else j = parent.startBinIdx;

        }

        if (j < cnts.length) endBinIdx = j; else endBinIdx = j - 1;
      } else {
        startBinIdx = parent.startBinIdx;
        endBinIdx = parent.endBinIdx;
      }
      flowFlag = 0; // flow to left
    } else {
      if (parent.splitFlag) { // right child
        endBinIdx = parent.endBinIdx;

        int j = parent.startBinIdx;
        int count;
        if (parent.startBinIdx == 0 && parent.endBinIdx > parent.startBinIdx) {
          count = parent.cnts[parent.endBinIdx];
          while(j < endBinIdx && parent.cnts[j] < count / 2) {
            j += 1;
          }
        } else {
          if (parent.endBinIdx > parent.startBinIdx) {
            count = parent.cnts[parent.endBinIdx] - parent.cnts[parent.startBinIdx - 1];
            while(j < parent.endBinIdx && (parent.cnts[j] - parent.cnts[parent.startBinIdx - 1] ) < count / 2)
              j += 1;
          } else j = parent.startBinIdx;

        }

        startBinIdx = j + 1 > parent.endBinIdx ? parent.endBinIdx : j + 1;
      } else {
        startBinIdx = parent.startBinIdx;
        endBinIdx = parent.endBinIdx;
      }
      flowFlag = 1; // flow to right
    }
  }

  public void accumulate(int index, SumPair sumPair) {
    cnts[index] += sumPair.getCnt();
  }

  public Histogram plus(Histogram other) {
    Histogram res = new Histogram(numBin);
    for (int i = 0; i < numBin; i++) {
      res.cnts[i] = this.cnts[i] + other.cnts[i];
    }
    return res;
  }

  public void plusBy(Histogram other) {
    for (int i = 0; i < numBin; i++) {
      this.cnts[i] += other.cnts[i];
    }
  }

  public Histogram subtract(Histogram other) {
    Histogram res = new Histogram(numBin);
    for (int i = 0; i < numBin; i++) {
      res.cnts[i] = this.cnts[i] - other.cnts[i];
    }
    return res;
  }

  public void subtractBy(Histogram other) {
    for (int i = 0; i < numBin; i++) {
      this.cnts[i] -= other.cnts[i];
    }

    this.flowFlag = 1 - other.flowFlag;
    if (this.splitFlag) {
      if (this.flowFlag == 1) {
        this.startBinIdx = other.endBinIdx + 1;
      } else {
        this.endBinIdx = other.startBinIdx - 1;
      }
      this.splitFlag = false;
    }

  }

  public SumPair sum() {
    return sum(0, numBin);
  }

  public SumPair sum(int start, int end) {
    float sumValue = 0.0f;
    float sumVariance = 0.0f;
    int sumCnt = 0;
    for (int i = start; i < end; i++) {
      sumCnt += cnts[i];
    }
    return new SumPair(sumCnt);
  }

  public int getNumBin() { return numBin;}

  public int getSplitFlag() { return flowFlag;}

  public int getStartBinIdx() {
    return startBinIdx;
  }

  public int getEndBinIdx() {
    return endBinIdx;
  }

  public boolean isSplitFlag() {
    return splitFlag;
  }

  public void setSplitFlag(boolean splitFlag) {
    this.splitFlag = splitFlag;
  }


  public SumPair get(int index) {
    return new SumPair(cnts[index]);
  }

  public void put(int index, SumPair sp) {
    sp.set(cnts[index]);
  }

  public void plusTo(SumPair sp, int index) {
    sp.plusBy(cnts[index]);
  }

  public void subtractTo(SumPair sp, int index) {
    sp.subtractBy(cnts[index]);
  }

  public void scan(int index, SumPair left, SumPair right) {
    if (cnts[index] <= right.getCnt()) {
      left.plusBy(cnts[index]);
      right.subtractBy(cnts[index]);
    }

  }

  public void clear() {
    Arrays.fill(cnts, 0);
  }

}
