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

package com.tencent.angel.spark.ml.tree.sketch;

import java.util.Arrays;
import java.util.Random;

public class SketchUtils {

  private static final Random rand = new Random();

  public static void checkK(int k) {
    if (k < 1) {
      throw new QuantileSketchException("Invalid value of k: k should be positive");
    } else if (k >= 65535) {
      throw new QuantileSketchException("Invalid value of k: k should not be larger than 65536");
    } else if (!isPowerOf2(k)) {
      throw new QuantileSketchException("Invalid value of k: k should be power of 2");
    }
  }


  public static boolean isPowerOf2(int k) {
    for (int i = 1; i < 65536; i <<= 1) {
      if (k == i) {
        return true;
      }
    }
    return false;
  }

  public static int needBufferCapacity(int k, long estimateN) {
    int numLevels = 1 + (63 - Long.numberOfLeadingZeros(estimateN / (k * 2)));
    return k * (numLevels + 2);
  }

  protected static void checkBitPattern(long bitPattern, long n, int k) {
    if (bitPattern != n / (k * 2)) {
      throw new QuantileSketchException("Bit Pattern not match");
    }
  }

  protected static void checkFraction(float fraction) {
    if (fraction < 0.0 || fraction > 1.0) {
      throw new QuantileSketchException("Fraction should be in range [0.0, 1.0]");
    }
  }

  protected static void checkFractions(float[] fractions) {
    for (float f : fractions) {
      checkFraction(f);
    }
  }

  protected static void checkEvenPartiotion(int evenPartition) {
    if (evenPartition <= 0) {
      throw new QuantileSketchException("Invalid partition number: " + evenPartition);
    }
  }

  protected static void compactBuffer(final float[] srcBuf, int srcOffset,
      final float[] dstBuf, int dstOffset, int dstSize) {
    int offset = rand.nextBoolean() ? 1 : 0;
    int bound = dstOffset + dstSize;
    for (int i = srcOffset + offset, j = dstOffset; j < bound; i += 2, j++) {
      dstBuf[j] = srcBuf[i];
    }
  }

  protected static void mergeArrays(final float[] src1, int srcOffset1,
      final float[] src2, int srcOffset2,
      final float[] dst, int dstOffset, int size) {
    int bound1 = srcOffset1 + size;
    int bound2 = srcOffset2 + size;
    int i1 = srcOffset1, i2 = srcOffset2, i3 = dstOffset;
    while (i1 < bound1 && i2 < bound2) {
      if (src1[i1] < src2[i2]) {
        dst[i3++] = src1[i1++];
      } else {
        dst[i3++] = src2[i2++];
      }
    }
    if (i1 < bound1) {
      System.arraycopy(src1, i1, dst, i3, bound1 - i1);
    } else {
      System.arraycopy(src2, i2, dst, i3, bound2 - i2);
    }
  }

  protected static void levelwisePropagation(long bitPattern, int k,
      int beginLevel, int endLevel,
      final float[] buf, int bufBeginPos,
      final float[] levelsArr) {
    for (int level = beginLevel; level < endLevel; level++) {
      if ((bitPattern & (1L << level)) == 0) {
        throw new QuantileSketchException("Encounter empty level: " + level);
      }
      SketchUtils.mergeArrays(levelsArr, k * (level + 2),
          levelsArr, k * (endLevel + 2), buf, bufBeginPos, k);
      SketchUtils.compactBuffer(buf, bufBeginPos, levelsArr, k * (endLevel + 2), k);
    }
  }

  protected static void blockyMergeSort(final float[] keys, final long[] values,
      int length, int blkSize) {
    if (blkSize <= 0 || length <= blkSize) {
      return;
    }
    int numBlks = (length + (blkSize - 1)) / blkSize;
    final float[] tmpKeys = Arrays.copyOf(keys, length);
    final long[] tmpValues = Arrays.copyOf(values, length);
    recursiveBlockyMergeSort(tmpKeys, tmpValues, keys, values, 0, numBlks, blkSize, length);
  }

  protected static void recursiveBlockyMergeSort(final float[] kSrc, final long[] vSrc,
      final float[] kDst, final long[] vDst,
      int blkStart, int blkLen, int blkSize, int arrLimit) {
    if (blkLen == 1) {
      return;
    }
    int blkLen1 = blkLen >> 1;
    int blkLen2 = blkLen - blkLen1;
    int blkStart1 = blkStart;
    int blkStart2 = blkStart + blkLen1;

    recursiveBlockyMergeSort(kDst, vDst, kSrc, vSrc, blkStart1, blkLen1, blkSize, arrLimit);
    recursiveBlockyMergeSort(kDst, vDst, kSrc, vSrc, blkStart2, blkLen2, blkSize, arrLimit);

    int arrStart1 = blkStart1 * blkSize;
    int arrStart2 = blkStart2 * blkSize;
    int arrLen1 = blkLen1 * blkSize;
    int arrLen2 = blkLen2 * blkSize;
    if (arrStart2 + arrLen2 > arrLimit) {
      arrLen2 = arrLimit - arrStart2;
    }

    blockyMerge(kSrc, vSrc, arrStart1, arrLen1, arrStart2, arrLen2, kDst, vDst, arrStart1);
  }

  protected static void blockyMerge(final float[] kSrc, final long[] vSrc,
      int arrStart1, int arrLen1,
      int arrStart2, int arrLen2,
      final float[] kDst, final long[] vDst, int arrStart3) {
    int arrEnd1 = arrStart1 + arrLen1;
    int arrEnd2 = arrStart2 + arrLen2;
    int i1 = arrStart1, i2 = arrStart2, i3 = arrStart3;
    while (i1 < arrEnd1 && i2 < arrEnd2) {
      if (kSrc[i1] <= kSrc[i2]) {
        kDst[i3] = kSrc[i1];
        vDst[i3] = vSrc[i1];
        ++i1;
        ++i3;
      } else {
        kDst[i3] = kSrc[i2];
        vDst[i3] = vSrc[i2];
        ++i2;
        ++i3;
      }
    }

    if (i1 < arrEnd1) {
      System.arraycopy(kSrc, i1, kDst, i3, arrEnd1 - i1);
      System.arraycopy(vSrc, i1, vDst, i3, arrEnd1 - i1);
    } else {
      System.arraycopy(kSrc, i2, kDst, i3, arrEnd2 - i2);
      System.arraycopy(vSrc, i2, vDst, i3, arrEnd2 - i2);
    }
  }
}

