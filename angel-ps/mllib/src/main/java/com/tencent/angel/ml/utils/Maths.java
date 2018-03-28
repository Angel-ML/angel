/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.ml.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Maths {

  public static float sigmoid(float x) {
    return (float) (1.0 / (1.0 + Math.exp(-x)));
  }

  public static double sigmoid(double x) {
    return (1.0 / (1.0 + Math.exp(-x)));
  }

  public static int sqr(int x) {
    return x * x;
  }

  public static float sqr(float x) {
    return x * x;
  }

  public static double sqr(double x) {
    return x * x;
  }

  public static void softmax(double[] rec) {
    double wmax = rec[0];
    for (int i = 1; i < rec.length; ++i) {
      wmax = Math.max(rec[i], wmax);
    }
    double wsum = 0.0;
    for (int i = 0; i < rec.length; ++i) {
      rec[i] = Math.exp(rec[i] - wmax);
      wsum += rec[i];
    }
    for (int i = 0; i < rec.length; ++i) {
      rec[i] /= wsum;
    }
  }

  public static void softmax(float[] rec) {
    float wmax = rec[0];
    for (int i = 1; i < rec.length; ++i) {
      wmax = Math.max(rec[i], wmax);
    }
    float wsum = 0.0f;
    for (int i = 0; i < rec.length; ++i) {
      rec[i] = (float) Math.exp(rec[i] - wmax);
      wsum += rec[i];
    }
    for (int i = 0; i < rec.length; ++i) {
      rec[i] /= wsum;
    }
  }

  public static double thresholdL1(double w, double lambda) {
    if (w > +lambda)
      return w - lambda;
    if (w < -lambda)
      return w + lambda;
    return 0.0;
  }

  public static float thresholdL1(float w, float lambda) {
    if (w > +lambda)
      return w - lambda;
    if (w < -lambda)
      return w + lambda;
    return 0.0f;
  }

  public static boolean isEven(int v) {
    return v % 2 == 0;
  }

  public static int pow(int a, int b) {
    if (b == 0)
      return 1;
    if (b == 1)
      return a;
    if (isEven(b))
      return pow(a * a, b / 2); // even a=(a^2)^b/2
    else
      return a * pow(a * a, b / 2); // odd a=a*(a^2)^b/2

  }

  public static void shuffle(int[] array) {
    int index, temp;
    Random random = new Random();
    for (int i = array.length - 1; i > 0; i--) {
      index = random.nextInt(i + 1);
      temp = array[index];
      array[index] = array[i];
      array[i] = temp;
    }
  }

  public static int[] intList2Arr(List<Integer> integers) {
    int[] ret = new int[integers.size()];
    for (int i = 0; i < integers.size(); i++) {
      ret[i] = integers.get(i);
    }
    return ret;
  }

  public static float[] floatList2Arr(List<Float> floats) {
    float[] ret = new float[floats.size()];
    for (int i = 0; i < floats.size(); i++) {
      ret[i] = floats.get(i);
    }
    return ret;
  }

  public static long[] longList2Arr(List<Long> longs) {
    long[] ret = new long[longs.size()];
    for (int i = 0; i < longs.size(); i++) {
      ret[i] = longs.get(i);
    }
    return ret;
  }

  public static int[] list2Arr(List<Integer> nzzIdxes) {
    // TODO Auto-generated method stub
    return null;
  }

  public static int findMaxIndex(float[] floats) {
    int rec = 0;
    float max = floats[rec];
    for (int i = 1; i < floats.length; i++) {
      if (floats[i] > max) {
        rec = i;
        max = floats[i];
      }
    }
    return rec;
  }

  public static float[] double2Float(double[] doubles) {
    float[] ret = new float[doubles.length];
    for (int i = 0; i < doubles.length; i++) {
      ret[i] = (float) doubles[i];
    }
    return ret;
  }

  public static void main(String[] argv) {
    float[] tmp = {-2.0f, -1.0f, 0.0f, 1.0f, 2.0f};
    softmax(tmp);
    System.out.println(Arrays.toString(tmp));
  }
}
