/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ml.math2.utils;


public class ArrayCopy {

  public static double[] copy(double[] src) {
    double[] dest = new double[src.length];
    System.arraycopy(src, 0, dest, 0, src.length);
    return dest;
  }

  public static double[] copy(double[] src, double[] dest) {
    System.arraycopy(src, 0, dest, 0, src.length);
    return dest;
  }

  public static double[] copy(float[] src, double[] dest) {
    for (int i = 0; i < src.length; i++) {
      dest[i] = src[i];
    }

    return dest;
  }

  public static double[] copy(float[] src, int sstart, double[] dest, int dstart, int len) {
    for (int i = 0; i < len; i++) {
      dest[dstart + i] = src[sstart + i];
    }
    return dest;
  }

  public static double[] copy(long[] src, double[] dest) {
    for (int i = 0; i < src.length; i++) {
      dest[i] = src[i];
    }

    return dest;
  }

  public static double[] copy(long[] src, int sstart, double[] dest, int dstart, int len) {
    for (int i = 0; i < len; i++) {
      dest[dstart + i] = src[sstart + i];
    }
    return dest;
  }

  public static double[] copy(int[] src, double[] dest) {
    for (int i = 0; i < src.length; i++) {
      dest[i] = src[i];
    }

    return dest;
  }

  public static double[] copy(int[] src, int sstart, double[] dest, int dstart, int len) {
    for (int i = 0; i < len; i++) {
      dest[dstart + i] = src[sstart + i];
    }
    return dest;
  }

  public static float[] copy(float[] src, float[] dest) {
    System.arraycopy(src, 0, dest, 0, src.length);
    return dest;
  }

  public static float[] copy(float[] src) {
    float[] dest = new float[src.length];
    System.arraycopy(src, 0, dest, 0, src.length);
    return dest;
  }

  public static float[] copy(long[] src, float[] dest) {
    for (int i = 0; i < src.length; i++) {
      dest[i] = src[i];
    }
    return dest;
  }

  public static float[] copy(long[] src, int sstart, float[] dest, int dstart, int len) {
    for (int i = 0; i < len; i++) {
      dest[dstart + i] = src[sstart + i];
    }
    return dest;
  }

  public static float[] copy(int[] src, float[] dest) {
    for (int i = 0; i < src.length; i++) {
      dest[i] = src[i];
    }
    return dest;
  }

  public static float[] copy(int[] src, int sstart, float[] dest, int dstart, int len) {
    for (int i = 0; i < len; i++) {
      dest[dstart + i] = src[sstart + i];
    }
    return dest;
  }

  public static long[] copy(long[] src, long[] dest) {
    System.arraycopy(src, 0, dest, 0, src.length);
    return dest;
  }

  public static long[] copy(long[] src) {
    long[] dest = new long[src.length];
    System.arraycopy(src, 0, dest, 0, src.length);
    return dest;
  }

  public static long[] copy(int[] src, long[] dest) {
    for (int i = 0; i < src.length; i++) {
      dest[i] = src[i];
    }
    return dest;
  }

  public static long[] copy(int[] src, int sstart, long[] dest, int dstart, int len) {
    for (int i = 0; i < len; i++) {
      dest[dstart + i] = src[sstart + i];
    }
    return dest;
  }

  public static int[] copy(int[] src, int[] dest) {
    System.arraycopy(src, 0, dest, 0, src.length);
    return dest;
  }

  public static int[] copy(int[] src) {
    int[] dest = new int[src.length];
    System.arraycopy(src, 0, dest, 0, src.length);
    return dest;
  }
}