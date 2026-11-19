package com.tencent.angel.spark.ml.tree.kdtree.histogram;

import java.io.Serializable;

public class SumPair implements Serializable {

  private int cnt;

  public SumPair() {
  }

  public SumPair(int cnt) {
    this.cnt = cnt;
  }

  public void plusBy(SumPair sumPair) {
    this.cnt += sumPair.cnt;
  }

  public void plusBy(int cnt) {
    this.cnt += cnt;
  }


  public void subtractBy(SumPair sumPair) {
    this.cnt -= sumPair.cnt;
  }

  public void subtractBy(int cnt) {
    this.cnt -= cnt;
  }

  public SumPair plus(SumPair sumPair) {
    SumPair res = this.copy();
    res.plusBy(sumPair);
    return res;
  }

  public SumPair plus(int cnt) {
    return new SumPair(this.cnt + cnt);
  }

  public SumPair subtract(SumPair sumPair) {
    SumPair res = this.copy();
    res.subtractBy(sumPair);
    return res;
  }

  public SumPair subtract(int cnt) {
    return new SumPair(this.cnt - cnt);
  }

  public int getCnt() {
    return cnt;
  }

  public void setCnt(int cnt) {
    this.cnt = cnt;
  }


  public void set(int cnt) {
    this.cnt = cnt;
  }
  
  public SumPair copy() {
    return new SumPair(cnt);
  }

  public String toString() {
    return "(" + cnt + ")";
  }

}
