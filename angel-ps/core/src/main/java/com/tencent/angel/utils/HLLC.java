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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.utils;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import net.agkn.hll.HLL;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * A distinct estimate tool
 */
public class HLLC {
  private final static Log LOG = LogFactory.getLog(HLLC.class);

  public static long distinct(Long2DoubleOpenHashMap [] maps) {
    DistinctOp op = new DistinctOp(maps, 0, maps.length);
    ForkJoinPool pool = new ForkJoinPool(16);
    pool.execute(op);
    long value = op.join().cardinality();
    pool.shutdownNow();
    return value;
  }

  static class DistinctOp extends RecursiveTask<HLL> {
    private final Long2DoubleOpenHashMap[] splits;
    private final int startPos;
    private final int endPos;

    public DistinctOp(Long2DoubleOpenHashMap[] splits, int startPos, int endPos) {
      this.splits = splits;
      this.startPos = startPos;
      this.endPos = endPos;
    }
    @Override protected HLL compute() {
      if (endPos <= startPos) {
        return new HLL(13, 5);
      }

      if (endPos - startPos == 1) {
        if (splits[startPos] != null) {
          return genHll(splits[startPos]);
        } else {
          return new HLL(13, 5);
        }
      } else {
        int middle = (startPos + endPos) / 2;
        DistinctOp
          opLeft = new DistinctOp(splits, startPos, middle);
        DistinctOp
          opRight = new DistinctOp(splits, middle, endPos);
        invokeAll(opLeft, opRight);

        try {
          HLL left = opLeft.get();
          left.union(opRight.get());
          return left;
        } catch (InterruptedException | ExecutionException e) {
          LOG.error("DistinctOp failed " + e.getMessage());
          return new HLL(13, 5);
        }
      }
    }

    private HLL genHll(Long2DoubleOpenHashMap map) {
      HLL hll = new HLL(13, 5);
      ObjectIterator<Long2DoubleMap.Entry> iter = map.long2DoubleEntrySet().fastIterator();
      while(iter.hasNext()) {
        hll.addRaw(hash(iter.next().getLongKey()));
      }
      return hll;
    }

    private static long hash(long value) {
      String str = String.valueOf(value);
      byte [] data = str.getBytes();
      return MurmurHash3.murmurhash3_x64_64(data, data.length, 123456);
    }
  }
}
