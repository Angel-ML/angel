package com.tencent.angel.common.collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

/**
 * Unit tests for DynamicArray family: serialize/deserialize round-trip, bufferLen consistency,
 * getData correctness, and batch-boundary edge cases.
 *
 * <p>NOTE: setBatchSize() is NOT used because it changes batchSize AFTER the first batch is
 * already allocated with the old size, creating an inconsistency. In production, batchSize is
 * always 64 (the default) and never changed. To test multi-batch behaviour we simply add more
 * than 64 elements.
 */
public class DynamicArrayTest {

  // ================================================================
  // DynamicLongArray
  // ================================================================

  @Test
  public void testDynamicLongArray_empty() {
    DynamicLongArray arr = new DynamicLongArray(16);
    assertEquals(0, arr.size());
    assertArrayEquals(new long[0], arr.getData());
    assertEquals(4, arr.bufferLen()); // INT_LENGTH only
  }

  @Test
  public void testDynamicLongArray_small() {
    DynamicLongArray arr = new DynamicLongArray(16);
    long[] expected = {1L, 2L, 3L, 10L, Long.MAX_VALUE, Long.MIN_VALUE};
    for (long v : expected) arr.add(v);

    assertEquals(expected.length, arr.size());
    assertArrayEquals(expected, arr.getData());

    ByteBuf buf = Unpooled.buffer(arr.bufferLen());
    arr.serialize(buf);
    assertEquals(arr.bufferLen(), buf.readableBytes());

    DynamicLongArray arr2 = new DynamicLongArray();
    arr2.deserialize(buf);
    assertArrayEquals(expected, arr2.getData());
    assertEquals(expected.length, arr2.size());
  }

  @Test
  public void testDynamicLongArray_multiBatch() {
    DynamicLongArray arr = new DynamicLongArray(0);
    int n = 200; // > 64 (default batchSize) to force multiple batches
    long[] expected = new long[n];
    for (int i = 0; i < n; i++) {
      expected[i] = (long) i * 7 + 3;
      arr.add(expected[i]);
    }

    assertEquals(n, arr.size());
    assertArrayEquals(expected, arr.getData());

    ByteBuf buf = Unpooled.buffer(arr.bufferLen());
    arr.serialize(buf);
    assertEquals(arr.bufferLen(), buf.readableBytes());

    DynamicLongArray arr2 = new DynamicLongArray();
    arr2.deserialize(buf);
    assertArrayEquals(expected, arr2.getData());
  }

  @Test
  public void testDynamicLongArray_exactBatchBoundary() {
    DynamicLongArray arr = new DynamicLongArray(0);
    // Default batchSize=64; fill exactly one batch then one more element
    for (int i = 0; i < 64; i++) arr.add((long) i);
    assertEquals(64, arr.size());

    arr.add(64L);
    assertEquals(65, arr.size());

    long[] expected = new long[65];
    for (int i = 0; i < 65; i++) expected[i] = i;
    assertArrayEquals(expected, arr.getData());

    ByteBuf buf = Unpooled.buffer(arr.bufferLen());
    arr.serialize(buf);
    assertEquals(arr.bufferLen(), buf.readableBytes());
    DynamicLongArray arr2 = new DynamicLongArray();
    arr2.deserialize(buf);
    assertArrayEquals(expected, arr2.getData());
  }

  // ================================================================
  // DynamicIntArray
  // ================================================================

  @Test
  public void testDynamicIntArray_roundTrip() {
    DynamicIntArray arr = new DynamicIntArray(16);
    int[] expected = {0, 1, 255, -1, Integer.MAX_VALUE, Integer.MIN_VALUE};
    for (int v : expected) arr.add(v);

    assertEquals(expected.length, arr.size());
    assertArrayEquals(expected, arr.getData());

    ByteBuf buf = Unpooled.buffer(arr.bufferLen());
    arr.serialize(buf);
    assertEquals(arr.bufferLen(), buf.readableBytes());

    DynamicIntArray arr2 = new DynamicIntArray();
    arr2.deserialize(buf);
    assertArrayEquals(expected, arr2.getData());
  }

  @Test
  public void testDynamicIntArray_multiBatch() {
    DynamicIntArray arr = new DynamicIntArray(0);
    int n = 200;
    int[] expected = new int[n];
    for (int i = 0; i < n; i++) {
      expected[i] = i * 3 + 1;
      arr.add(expected[i]);
    }
    assertEquals(n, arr.size());
    assertArrayEquals(expected, arr.getData());

    ByteBuf buf = Unpooled.buffer(arr.bufferLen());
    arr.serialize(buf);
    assertEquals(arr.bufferLen(), buf.readableBytes());
    DynamicIntArray arr2 = new DynamicIntArray();
    arr2.deserialize(buf);
    assertArrayEquals(expected, arr2.getData());
  }

  // ================================================================
  // DynamicFloatArray
  // ================================================================

  @Test
  public void testDynamicFloatArray_roundTrip() {
    DynamicFloatArray arr = new DynamicFloatArray(16);
    float[] expected = {0.0f, 1.5f, -3.14f, Float.MAX_VALUE, Float.MIN_VALUE};
    for (float v : expected) arr.add(v);

    assertEquals(expected.length, arr.size());
    assertArrayEquals(expected, arr.getData(), 0.0f);

    ByteBuf buf = Unpooled.buffer(arr.bufferLen());
    arr.serialize(buf);
    assertEquals(arr.bufferLen(), buf.readableBytes());

    DynamicFloatArray arr2 = new DynamicFloatArray();
    arr2.deserialize(buf);
    assertArrayEquals(expected, arr2.getData(), 0.0f);
  }

  // ================================================================
  // DynamicDoubleArray
  // ================================================================

  @Test
  public void testDynamicDoubleArray_roundTrip() {
    DynamicDoubleArray arr = new DynamicDoubleArray(16);
    double[] expected = {0.0, 1.5, -3.14, Double.MAX_VALUE, Double.MIN_VALUE};
    for (double v : expected) arr.add(v);

    assertEquals(expected.length, arr.size());
    assertArrayEquals(expected, arr.getData(), 0.0);

    ByteBuf buf = Unpooled.buffer(arr.bufferLen());
    arr.serialize(buf);
    assertEquals(arr.bufferLen(), buf.readableBytes());

    DynamicDoubleArray arr2 = new DynamicDoubleArray();
    arr2.deserialize(buf);
    assertArrayEquals(expected, arr2.getData(), 0.0);
  }

  // ================================================================
  // DynamicObjectArray (with DummyElement)
  // ================================================================

  @Test
  public void testDynamicObjectArray_roundTrip() {
    DynamicObjectArray arr = new DynamicObjectArray(16);
    DummyElement e1 = new DummyElement();
    DummyElement e2 = new DummyElement();
    arr.add(e1);
    arr.add(e2);
    arr.add(null);

    assertEquals(3, arr.size());

    ByteBuf buf = Unpooled.buffer(arr.bufferLen());
    arr.serialize(buf);
    assertEquals(arr.bufferLen(), buf.readableBytes());

    DynamicObjectArray arr2 = new DynamicObjectArray();
    arr2.deserialize(buf);
    assertEquals(3, arr2.size());
  }

  @Test
  public void testDynamicObjectArray_empty() {
    DynamicObjectArray arr = new DynamicObjectArray(16);
    assertEquals(0, arr.size());
    assertEquals(4, arr.bufferLen());
  }

  // ================================================================
  // DynamicLongFloatArrayPair
  // ================================================================

  @Test
  public void testDynamicLongFloatArrayPair_roundTrip() {
    DynamicLongFloatArrayPair pair = new DynamicLongFloatArrayPair(16);
    long[] keys = {1L, 2L, 3L, 100L, Long.MAX_VALUE};
    float[] vals = {0.1f, 0.2f, 0.3f, -1.0f, Float.MAX_VALUE};
    for (int i = 0; i < keys.length; i++) {
      pair.add(keys[i], vals[i]);
    }

    assertEquals(keys.length, pair.size());
    assertArrayEquals(keys, pair.getKeys());
    assertArrayEquals(vals, pair.getValues(), 0.0f);

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    assertEquals(pair.bufferLen(), buf.readableBytes());

    DynamicLongFloatArrayPair pair2 = new DynamicLongFloatArrayPair();
    pair2.deserialize(buf);
    assertEquals(keys.length, pair2.size());
    assertArrayEquals(keys, pair2.getKeys());
    assertArrayEquals(vals, pair2.getValues(), 0.0f);
  }

  @Test
  public void testDynamicLongFloatArrayPair_multiBatch() {
    DynamicLongFloatArrayPair pair = new DynamicLongFloatArrayPair(0);
    int n = 200; // > 64 to force multiple batches
    long[] keys = new long[n];
    float[] vals = new float[n];
    for (int i = 0; i < n; i++) {
      keys[i] = (long) i;
      vals[i] = i * 0.1f;
      pair.add(keys[i], vals[i]);
    }

    assertEquals(n, pair.size());
    assertArrayEquals(keys, pair.getKeys());
    assertArrayEquals(vals, pair.getValues(), 0.0f);

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    assertEquals(pair.bufferLen(), buf.readableBytes());

    DynamicLongFloatArrayPair pair2 = new DynamicLongFloatArrayPair();
    pair2.deserialize(buf);
    assertArrayEquals(keys, pair2.getKeys());
    assertArrayEquals(vals, pair2.getValues(), 0.0f);
  }

  // ================================================================
  // DynamicLongIntArrayPair
  // ================================================================

  @Test
  public void testDynamicLongIntArrayPair_roundTrip() {
    DynamicLongIntArrayPair pair = new DynamicLongIntArrayPair(16);
    long[] keys = {10L, 20L, 30L};
    int[] vals = {100, 200, 300};
    for (int i = 0; i < keys.length; i++) {
      pair.add(keys[i], vals[i]);
    }

    assertEquals(keys.length, pair.size());
    assertArrayEquals(keys, pair.getKeys());
    assertArrayEquals(vals, pair.getValues());

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    assertEquals(pair.bufferLen(), buf.readableBytes());

    DynamicLongIntArrayPair pair2 = new DynamicLongIntArrayPair();
    pair2.deserialize(buf);
    assertArrayEquals(keys, pair2.getKeys());
    assertArrayEquals(vals, pair2.getValues());
  }

  @Test
  public void testDynamicLongIntArrayPair_multiBatch() {
    DynamicLongIntArrayPair pair = new DynamicLongIntArrayPair(0);
    int n = 200;
    long[] keys = new long[n];
    int[] vals = new int[n];
    for (int i = 0; i < n; i++) {
      keys[i] = (long) i;
      vals[i] = i * 2;
      pair.add(keys[i], vals[i]);
    }
    assertEquals(n, pair.size());
    assertArrayEquals(keys, pair.getKeys());
    assertArrayEquals(vals, pair.getValues());

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    assertEquals(pair.bufferLen(), buf.readableBytes());
    DynamicLongIntArrayPair pair2 = new DynamicLongIntArrayPair();
    pair2.deserialize(buf);
    assertArrayEquals(keys, pair2.getKeys());
    assertArrayEquals(vals, pair2.getValues());
  }

  // ================================================================
  // DynamicLongLongArrayPair
  // ================================================================

  @Test
  public void testDynamicLongLongArrayPair_roundTrip() {
    DynamicLongLongArrayPair pair = new DynamicLongLongArrayPair(16);
    long[] keys = {1L, 2L, 3L};
    long[] vals = {10L, 20L, 30L};
    for (int i = 0; i < keys.length; i++) {
      pair.add(keys[i], vals[i]);
    }

    assertEquals(keys.length, pair.size());
    assertArrayEquals(keys, pair.getKeys());
    assertArrayEquals(vals, pair.getValues());

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    assertEquals(pair.bufferLen(), buf.readableBytes());

    DynamicLongLongArrayPair pair2 = new DynamicLongLongArrayPair();
    pair2.deserialize(buf);
    assertArrayEquals(keys, pair2.getKeys());
    assertArrayEquals(vals, pair2.getValues());
  }

  @Test
  public void testDynamicLongLongArrayPair_multiBatch() {
    DynamicLongLongArrayPair pair = new DynamicLongLongArrayPair(0);
    int n = 200;
    long[] keys = new long[n];
    long[] vals = new long[n];
    for (int i = 0; i < n; i++) {
      keys[i] = (long) i;
      vals[i] = (long) i * 3;
      pair.add(keys[i], vals[i]);
    }
    assertEquals(n, pair.size());
    assertArrayEquals(keys, pair.getKeys());
    assertArrayEquals(vals, pair.getValues());

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    DynamicLongLongArrayPair pair2 = new DynamicLongLongArrayPair();
    pair2.deserialize(buf);
    assertArrayEquals(keys, pair2.getKeys());
    assertArrayEquals(vals, pair2.getValues());
  }

  // ================================================================
  // DynamicLongDoubleArrayPair
  // ================================================================

  @Test
  public void testDynamicLongDoubleArrayPair_roundTrip() {
    DynamicLongDoubleArrayPair pair = new DynamicLongDoubleArrayPair(16);
    long[] keys = {1L, 2L, 3L};
    double[] vals = {1.0, 2.0, 3.0};
    for (int i = 0; i < keys.length; i++) {
      pair.add(keys[i], vals[i]);
    }

    assertEquals(keys.length, pair.size());

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    assertEquals(pair.bufferLen(), buf.readableBytes());

    DynamicLongDoubleArrayPair pair2 = new DynamicLongDoubleArrayPair();
    pair2.deserialize(buf);
    assertArrayEquals(keys, pair2.getKeys());
    assertArrayEquals(vals, pair2.getValues(), 0.0);
  }

  @Test
  public void testDynamicLongDoubleArrayPair_multiBatch() {
    DynamicLongDoubleArrayPair pair = new DynamicLongDoubleArrayPair(0);
    int n = 200;
    long[] keys = new long[n];
    double[] vals = new double[n];
    for (int i = 0; i < n; i++) {
      keys[i] = (long) i;
      vals[i] = i * 0.5;
      pair.add(keys[i], vals[i]);
    }
    assertEquals(n, pair.size());

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    DynamicLongDoubleArrayPair pair2 = new DynamicLongDoubleArrayPair();
    pair2.deserialize(buf);
    assertArrayEquals(keys, pair2.getKeys());
    assertArrayEquals(vals, pair2.getValues(), 0.0);
  }

  // ================================================================
  // DynamicLongObjectArrayPair (with DummyElement)
  // ================================================================

  @Test
  public void testDynamicLongObjectArrayPair_roundTrip() {
    DynamicLongObjectArrayPair pair = new DynamicLongObjectArrayPair(16);
    pair.add(1L, new DummyElement());
    pair.add(2L, new DummyElement());
    pair.add(3L, null);

    assertEquals(3, pair.size());

    long[] keys = pair.getKeys();
    assertArrayEquals(new long[]{1L, 2L, 3L}, keys);

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    assertEquals(pair.bufferLen(), buf.readableBytes());

    DynamicLongObjectArrayPair pair2 = new DynamicLongObjectArrayPair();
    pair2.deserialize(buf);
    assertEquals(3, pair2.size());
    assertArrayEquals(new long[]{1L, 2L, 3L}, pair2.getKeys());
  }

  // ================================================================
  // DynamicLongStringArrayPair
  // ================================================================

  @Test
  public void testDynamicLongStringArrayPair_roundTrip() {
    DynamicLongStringArrayPair pair = new DynamicLongStringArrayPair(16);
    long[] keys = {1L, 2L, 3L};
    String[] vals = {"a", "bb", "ccc"};
    for (int i = 0; i < keys.length; i++) {
      pair.add(keys[i], vals[i]);
    }

    assertEquals(keys.length, pair.size());

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    assertEquals(pair.bufferLen(), buf.readableBytes());

    DynamicLongStringArrayPair pair2 = new DynamicLongStringArrayPair();
    pair2.deserialize(buf);
    assertArrayEquals(keys, pair2.getKeys());
    assertArrayEquals(vals, pair2.getValues());
  }

  // ================================================================
  // DynamicIntFloatArrayPair
  // ================================================================

  @Test
  public void testDynamicIntFloatArrayPair_roundTrip() {
    DynamicIntFloatArrayPair pair = new DynamicIntFloatArrayPair(16);
    int[] keys = {1, 2, 3};
    float[] vals = {0.1f, 0.2f, 0.3f};
    for (int i = 0; i < keys.length; i++) {
      pair.add(keys[i], vals[i]);
    }

    assertEquals(keys.length, pair.size());

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    assertEquals(pair.bufferLen(), buf.readableBytes());

    DynamicIntFloatArrayPair pair2 = new DynamicIntFloatArrayPair();
    pair2.deserialize(buf);
    assertArrayEquals(keys, pair2.getKeys());
    assertArrayEquals(vals, pair2.getValues(), 0.0f);
  }

  @Test
  public void testDynamicIntFloatArrayPair_multiBatch() {
    DynamicIntFloatArrayPair pair = new DynamicIntFloatArrayPair(0);
    int n = 200;
    int[] keys = new int[n];
    float[] vals = new float[n];
    for (int i = 0; i < n; i++) {
      keys[i] = i;
      vals[i] = i * 0.2f;
      pair.add(keys[i], vals[i]);
    }
    assertEquals(n, pair.size());

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    DynamicIntFloatArrayPair pair2 = new DynamicIntFloatArrayPair();
    pair2.deserialize(buf);
    assertArrayEquals(keys, pair2.getKeys());
    assertArrayEquals(vals, pair2.getValues(), 0.0f);
  }

  // ================================================================
  // DynamicIntIntArrayPair
  // ================================================================

  @Test
  public void testDynamicIntIntArrayPair_roundTrip() {
    DynamicIntIntArrayPair pair = new DynamicIntIntArrayPair(16);
    int[] keys = {1, 2, 3};
    int[] vals = {10, 20, 30};
    for (int i = 0; i < keys.length; i++) {
      pair.add(keys[i], vals[i]);
    }

    assertEquals(keys.length, pair.size());

    ByteBuf buf = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf);
    assertEquals(pair.bufferLen(), buf.readableBytes());

    DynamicIntIntArrayPair pair2 = new DynamicIntIntArrayPair();
    pair2.deserialize(buf);
    assertArrayEquals(keys, pair2.getKeys());
    assertArrayEquals(vals, pair2.getValues());
  }

  // ================================================================
  // DynamicStringArray
  // ================================================================

  @Test
  public void testDynamicStringArray_roundTrip() {
    DynamicStringArray arr = new DynamicStringArray(16);
    String[] expected = {"hello", "world", "", "test123"};
    for (String s : expected) arr.add(s);

    assertEquals(expected.length, arr.size());
    assertArrayEquals(expected, arr.getData());

    ByteBuf buf = Unpooled.buffer(arr.bufferLen());
    arr.serialize(buf);
    assertEquals(arr.bufferLen(), buf.readableBytes());

    DynamicStringArray arr2 = new DynamicStringArray();
    arr2.deserialize(buf);
    assertArrayEquals(expected, arr2.getData());
  }

  // ================================================================
  // Cross-cutting: bufferLen == actual serialized bytes
  // ================================================================

  @Test
  public void testBufferLenAccuracy_allTypes() {
    DynamicLongArray dla = new DynamicLongArray(0);
    for (int i = 0; i < 100; i++) dla.add((long) i);
    ByteBuf dlaBuf = Unpooled.buffer(dla.bufferLen() + 100);
    dla.serialize(dlaBuf);
    assertEquals(dla.bufferLen(), dlaBuf.readableBytes());

    DynamicLongFloatArrayPair dlfap = new DynamicLongFloatArrayPair(0);
    for (int i = 0; i < 100; i++) dlfap.add((long) i, i * 0.1f);
    ByteBuf dlfapBuf = Unpooled.buffer(dlfap.bufferLen() + 100);
    dlfap.serialize(dlfapBuf);
    assertEquals(dlfap.bufferLen(), dlfapBuf.readableBytes());
  }

  // ================================================================
  // Edge case: single element
  // ================================================================

  @Test
  public void testSingleElement_allTypes() {
    DynamicLongArray dla = new DynamicLongArray(16);
    dla.add(42L);
    assertEquals(1, dla.size());
    assertArrayEquals(new long[]{42L}, dla.getData());
    ByteBuf buf1 = Unpooled.buffer(dla.bufferLen());
    dla.serialize(buf1);
    DynamicLongArray dla2 = new DynamicLongArray();
    dla2.deserialize(buf1);
    assertArrayEquals(new long[]{42L}, dla2.getData());

    DynamicLongFloatArrayPair pair = new DynamicLongFloatArrayPair(16);
    pair.add(42L, 0.99f);
    assertEquals(1, pair.size());
    ByteBuf buf2 = Unpooled.buffer(pair.bufferLen());
    pair.serialize(buf2);
    DynamicLongFloatArrayPair pair2 = new DynamicLongFloatArrayPair();
    pair2.deserialize(buf2);
    assertArrayEquals(new long[]{42L}, pair2.getKeys());
    assertArrayEquals(new float[]{0.99f}, pair2.getValues(), 0.0f);
  }
}
