package com.tencent.angel.ml.math2.utils;

import com.tencent.angel.ml.math2.vector.Vector;

public class LabeledData {

  private Vector x;
  private double y;
  private String attached;

  public LabeledData(Vector x, double y) {
    this(x, y, null);
  }

  public LabeledData(Vector x, double y, String attached) {
    this.x = x;
    this.y = y;
    this.attached = attached;
  }

  public LabeledData() {
    this(null, 0);
  }

  public Vector getX() {
    return x;
  }

  public void setX(Vector x) {
    this.x = x;
  }

  public double getY() {
    return y;
  }

  public void setY(double y) {
    this.y = y;
  }

  public void attach(String msg) {
    attached = msg;
  }

  public String getAttach() {
    return attached;
  }
}