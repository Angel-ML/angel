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
package com.tencent.angel.graph.client.node2vec.utils;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class SerDe {
  public static void serArray(int[] arr, ByteBuf buf) {
    assert buf != null;

    if (arr == null) {
      buf.writeInt(0);
    } else {
      buf.writeInt(arr.length);

      for (int e : arr) {
        buf.writeInt(e);
      }
    }
  }

  public static void serArray(int[] arr, int start, int end, ByteBuf buf) {
    assert buf != null;
    assert end >= start;

    if (arr == null) {
      buf.writeInt(0);
    } else {
      buf.writeInt(end - start);

      for (int i = start; i < end; i++) {
        buf.writeInt(arr[i]);
      }
    }
  }

  public static int[] deserIntArray(ByteBuf buf) {
    assert buf != null;

    int len = buf.readInt();
    if (len <= 0) {
      return null;
    } else {
      int[] arr = new int[len];
      for (int i = 0; i < len; i++) {
        arr[i] = buf.readInt();
      }
      return arr;
    }
  }

  public static int getArraySerSize(int[] arr) {
    if (arr == null) {
      return 4;
    } else {
      return 4 + 4 * arr.length;
    }
  }

  public static void serArray(long[] arr, ByteBuf buf) {
    assert buf != null;

    if (arr == null) {
      buf.writeInt(0);
    } else {
      buf.writeInt(arr.length);

      for (long e : arr) {
        buf.writeLong(e);
      }
    }
  }

  public static void serArray(long[] arr, int start, int end, ByteBuf buf) {
    assert buf != null;
    assert end >= start;

    if (arr == null) {
      buf.writeInt(0);
    } else {
      buf.writeInt(end - start);

      for (int i = start; i < end; i++) {
        buf.writeLong(arr[i]);
      }
    }
  }

  public static long[] deserLongArray(ByteBuf buf) {
    assert buf != null;

    int len = buf.readInt();
    if (len <= 0) {
      return null;
    } else {
      long[] arr = new long[len];
      for (int i = 0; i < len; i++) {
        arr[i] = buf.readLong();
      }
      return arr;
    }
  }

  public static int getArraySerSize(long[] arr) {
    if (arr == null) {
      return 4;
    } else {
      return 4 + 8 * arr.length;
    }
  }

  public static void serArray(float[] arr, ByteBuf buf) {
    assert buf != null;

    if (arr == null) {
      buf.writeInt(0);
    } else {
      buf.writeInt(arr.length);

      for (float e : arr) {
        buf.writeFloat(e);
      }
    }
  }

  public static void serArray(float[] arr, int start, int end, ByteBuf buf) {
    assert buf != null;
    assert end >= start;

    if (arr == null) {
      buf.writeInt(0);
    } else {
      buf.writeInt(end - start);

      for (int i = start; i < end; i++) {
        buf.writeFloat(arr[i]);
      }
    }
  }

  public static float[] deserFloatArray(ByteBuf buf) {
    assert buf != null;

    int len = buf.readInt();
    if (len <= 0) {
      return null;
    } else {
      float[] arr = new float[len];
      for (int i = 0; i < len; i++) {
        arr[i] = buf.readFloat();
      }
      return arr;
    }
  }

  public static int getArraySerSize(float[] arr) {
    if (arr == null) {
      return 4;
    } else {
      return 4 + 4 * arr.length;
    }
  }

  public static void serArray(double[] arr, ByteBuf buf) {
    assert buf != null;

    if (arr == null) {
      buf.writeInt(0);
    } else {
      buf.writeInt(arr.length);

      for (double e : arr) {
        buf.writeDouble(e);
      }
    }
  }

  public static void serArray(double[] arr, int start, int end, ByteBuf buf) {
    assert buf != null;
    assert end >= start;

    if (arr == null) {
      buf.writeInt(0);
    } else {
      buf.writeInt(end - start);

      for (int i = start; i < end; i++) {
        buf.writeDouble(arr[i]);
      }
    }
  }

  public static double[] deserDoubleArray(ByteBuf buf) {
    assert buf != null;

    int len = buf.readInt();
    if (len <= 0) {
      return null;
    } else {
      double[] arr = new double[len];
      for (int i = 0; i < len; i++) {
        arr[i] = buf.readDouble();
      }
      return arr;
    }
  }

  public static int getArraySerSize(double[] arr) {
    if (arr == null) {
      return 4;
    } else {
      return 4 + 8 * arr.length;
    }
  }

  public static <ARR> void serLong2ArrayHashMap(Long2ObjectOpenHashMap<ARR> obj, ByteBuf buf) {
    buf.writeInt(obj.size());

    ObjectIterator<Long2ObjectMap.Entry<ARR>> iter = obj.long2ObjectEntrySet().fastIterator();
    while (iter.hasNext()) {
      Long2ObjectMap.Entry<ARR> entry = iter.next();
      buf.writeLong(entry.getLongKey());
      ARR value = entry.getValue();
      switch (value.getClass().getSimpleName()) {
        case "int[]":
          serArray((int[]) value, buf);
          break;
        case "long[]":
          serArray((long[]) value, buf);
          break;
        case "float[]":
          serArray((float[]) value, buf);
          break;
        case "double[]":
          serArray((double[]) value, buf);
          break;
      }
    }
  }

  public static <ARR> void serLong2ArrayHashMap(long[] arr, int start, int end, Long2ObjectOpenHashMap<ARR> obj, ByteBuf buf) {
    buf.writeInt(end - start);

    for (int i = start; i < end; i++) {
      long key = arr[i];
      buf.writeLong(key);
      ARR value = obj.get(key);
      switch (value.getClass().getSimpleName()) {
        case "int[]":
          serArray((int[]) value, buf);
          break;
        case "long[]":
          serArray((long[]) value, buf);
          break;
        case "float[]":
          serArray((float[]) value, buf);
          break;
        case "double[]":
          serArray((double[]) value, buf);
          break;
      }
    }
  }

  public static <ARR> int getLong2ArrayHashMapSerSize(Long2ObjectOpenHashMap<ARR> obj) {
    if (obj == null) {
      return 4;
    } else {
      ObjectIterator<Long2ObjectMap.Entry<ARR>> iter = obj.long2ObjectEntrySet().fastIterator();
      int len = 4;
      while (iter.hasNext()) {
        Long2ObjectMap.Entry<ARR> entry = iter.next();
        ARR value = entry.getValue();
        switch (value.getClass().getSimpleName()) {
          case "int[]":
            len += 8 + getArraySerSize((int[]) value);
            break;
          case "long[]":
            len += 8 + getArraySerSize((long[]) value);
            break;
          case "float[]":
            len += 8 + getArraySerSize((float[]) value);
            break;
          case "double[]":
            len += 8 + getArraySerSize((double[]) value);
            break;
        }
      }

      return len;
    }
  }

  public static <ARR> int getLong2ArrayHashMapSerSize(long[] arr, int start, int end, Long2ObjectOpenHashMap<ARR> obj) {
    if (obj == null) {
      return 4;
    } else {
      int len = 4;
      for (int i = start; i < end; i++) {
        long key = arr[i];
        ARR value = obj.get(key);
        switch (value.getClass().getSimpleName()) {
          case "int[]":
            len += 8 + getArraySerSize((int[]) value);
            break;
          case "long[]":
            len += 8 + getArraySerSize((long[]) value);
            break;
          case "float[]":
            len += 8 + getArraySerSize((float[]) value);
            break;
          case "double[]":
            len += 8 + getArraySerSize((double[]) value);
            break;
        }
      }


      return len;
    }
  }

  public static Long2ObjectOpenHashMap<int[]> deserLong2IntArray(ByteBuf buf) {
    int size = buf.readInt();

    if (size <= 0) {
      return null;
    } else {
      Long2ObjectOpenHashMap<int[]> obj = new Long2ObjectOpenHashMap<>(size);

      for (int i = 0; i < size; i++) {
        obj.put(buf.readLong(), deserIntArray(buf));
      }

      return obj;
    }
  }

  public static Long2ObjectOpenHashMap<long[]> deserLong2LongArray(ByteBuf buf) {
    int size = buf.readInt();

    if (size <= 0) {
      return null;
    } else {
      Long2ObjectOpenHashMap<long[]> obj = new Long2ObjectOpenHashMap<>(size);

      for (int i = 0; i < size; i++) {
        obj.put(buf.readLong(), deserLongArray(buf));
      }

      return obj;
    }
  }

  public static Long2ObjectOpenHashMap<float[]> deserLong2FloatArray(ByteBuf buf) {
    int size = buf.readInt();

    if (size <= 0) {
      return null;
    } else {
      Long2ObjectOpenHashMap<float[]> obj = new Long2ObjectOpenHashMap<>(size);

      for (int i = 0; i < size; i++) {
        obj.put(buf.readLong(), deserFloatArray(buf));
      }

      return obj;
    }
  }

  public static Long2ObjectOpenHashMap<double[]> deserLong2DoubleArray(ByteBuf buf) {
    int size = buf.readInt();

    if (size <= 0) {
      return null;
    } else {
      Long2ObjectOpenHashMap<double[]> obj = new Long2ObjectOpenHashMap<>(size);

      for (int i = 0; i < size; i++) {
        obj.put(buf.readLong(), deserDoubleArray(buf));
      }

      return obj;
    }
  }

  public static <ARR> void serInt2ArrayHashMap(Int2ObjectOpenHashMap<ARR> obj, ByteBuf buf) {
    buf.writeInt(obj.size());

    ObjectIterator<Int2ObjectMap.Entry<ARR>> iter = obj.int2ObjectEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2ObjectMap.Entry<ARR> entry = iter.next();
      buf.writeInt(entry.getIntKey());
      ARR value = entry.getValue();
      switch (value.getClass().getSimpleName()) {
        case "int[]":
          serArray((int[]) entry.getValue(), buf);
          break;
        case "long[]":
          serArray((long[]) entry.getValue(), buf);
          break;
        case "float[]":
          serArray((float[]) entry.getValue(), buf);
          break;
        case "double[]":
          serArray((double[]) entry.getValue(), buf);
          break;
      }
    }
  }

  public static <ARR> void serInt2ArrayHashMap(int[] arr, int start, int end, Int2ObjectOpenHashMap<ARR> obj, ByteBuf buf) {
    buf.writeInt(end - start);

    for (int i = start; i < end; i++) {
      int key = arr[i];
      buf.writeInt(key);
      ARR value = obj.get(key);
      switch (value.getClass().getSimpleName()) {
        case "int[]":
          serArray((int[]) value, buf);
          break;
        case "long[]":
          serArray((long[]) value, buf);
          break;
        case "float[]":
          serArray((float[]) value, buf);
          break;
        case "double[]":
          serArray((double[]) value, buf);
          break;
      }
    }
  }

  public static <ARR> int getInt2ArrayHashMapSerSize(Int2ObjectOpenHashMap<ARR> obj) {
    if (obj == null) {
      return 4;
    } else {
      ObjectIterator<Int2ObjectMap.Entry<ARR>> iter = obj.int2ObjectEntrySet().fastIterator();
      int len = 4;
      while (iter.hasNext()) {
        Int2ObjectMap.Entry<ARR> entry = iter.next();
        ARR value = entry.getValue();
        switch (value.getClass().getSimpleName()) {
          case "int[]":
            len += 4 + getArraySerSize((int[]) value);
            break;
          case "long[]":
            len += 4 + getArraySerSize((long[]) value);
            break;
          case "float[]":
            len += 4 + getArraySerSize((float[]) value);
            break;
          case "double[]":
            len += 4 + getArraySerSize((double[]) value);
            break;
        }
      }

      return len;
    }
  }

  public static <ARR> int getInt2ArrayHashMapSerSize(int[] arr, int start, int end, Int2ObjectOpenHashMap<ARR> obj) {
    if (obj == null) {
      return 4;
    } else {
      int len = 4;
      for (int i = start; i < end; i++) {
        int key = arr[i];
        ARR value = obj.get(key);
        switch (value.getClass().getSimpleName()) {
          case "int[]":
            len += 4 + getArraySerSize((int[]) value);
            break;
          case "long[]":
            len += 4 + getArraySerSize((long[]) value);
            break;
          case "float[]":
            len += 4 + getArraySerSize((float[]) value);
            break;
          case "double[]":
            len += 4 + getArraySerSize((double[]) value);
            break;
        }
      }


      return len;
    }
  }

  public static Int2ObjectOpenHashMap<int[]> deserInt2IntArray(ByteBuf buf) {
    int size = buf.readInt();

    if (size <= 0) {
      return null;
    } else {
      Int2ObjectOpenHashMap<int[]> obj = new Int2ObjectOpenHashMap<>(size);

      for (int i = 0; i < size; i++) {
        obj.put(buf.readInt(), deserIntArray(buf));
      }

      return obj;
    }
  }

  public static Int2ObjectOpenHashMap<long[]> deserInt2LongArray(ByteBuf buf) {
    int size = buf.readInt();

    if (size <= 0) {
      return null;
    } else {
      Int2ObjectOpenHashMap<long[]> obj = new Int2ObjectOpenHashMap<>(size);

      for (int i = 0; i < size; i++) {
        obj.put(buf.readInt(), deserLongArray(buf));
      }

      return obj;
    }
  }

  public static Int2ObjectOpenHashMap<float[]> deserInt2FloatArray(ByteBuf buf) {
    int size = buf.readInt();

    if (size <= 0) {
      return null;
    } else {
      Int2ObjectOpenHashMap<float[]> obj = new Int2ObjectOpenHashMap<>(size);

      for (int i = 0; i < size; i++) {
        obj.put(buf.readInt(), deserFloatArray(buf));
      }

      return obj;
    }
  }

  public static Int2ObjectOpenHashMap<double[]> deserInt2DoubleArray(ByteBuf buf) {
    int size = buf.readInt();

    if (size <= 0) {
      return null;
    } else {
      Int2ObjectOpenHashMap<double[]> obj = new Int2ObjectOpenHashMap<>(size);

      for (int i = 0; i < size; i++) {
        obj.put(buf.readInt(), deserDoubleArray(buf));
      }

      return obj;
    }
  }

  public static void serInt2IntMap(Int2IntOpenHashMap map, ByteBuf buf) {
    if (map == null || map.size() == 0) {
      buf.writeInt(0);
    } else {
      buf.writeInt(map.size());
      ObjectIterator<Int2IntMap.Entry> iter = map.int2IntEntrySet().fastIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeInt(entry.getIntValue());
      }
    }
  }

  public static Int2IntOpenHashMap deserInt2IntMap(ByteBuf buf) {
    int size = buf.readInt();

    if (size == 0) {
      return null;
    }

    Int2IntOpenHashMap result = new Int2IntOpenHashMap(size);
    for (int i = 0; i < size; i++) {
      result.put(buf.readInt(), buf.readInt());
    }

    return result;
  }
}
