package com.tencent.angel.spark.ml.embedding;

public class Sigmoid {

  static int MAX_SIGMOID = 8;
  static int TABLE_SIZE = 1000;
  static float[] table = new float[TABLE_SIZE + 1];

  static {
    for (int i = 0; i < TABLE_SIZE; i ++) {
      table[i] = (float) Math.exp((i * 2.0 / TABLE_SIZE - 1) * MAX_SIGMOID);
      table[i] = table[i] / (table[i] + 1);
    }
  }

  public static float sigmoid(float x) {
    if (x >= MAX_SIGMOID) return 1.0f;
    else if (x < -MAX_SIGMOID) return 0.0f;
    else {
      return table[(int) ((x + MAX_SIGMOID) / (2 * MAX_SIGMOID) * TABLE_SIZE)];
    }
  }
}
