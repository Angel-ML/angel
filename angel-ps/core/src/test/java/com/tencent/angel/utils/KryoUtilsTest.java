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


package com.tencent.angel.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;
import com.tencent.angel.ml.metric.ErrorMetric;
import com.tencent.angel.ml.metric.LossMetric;
import com.tencent.angel.ml.metric.Metric;
import com.tencent.angel.ml.metric.ObjMetric;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KryoUtilsTest {
  public static class NotAMetric {
    public String marker;
    public int payload;
  }

  public static class LinkedMetric extends Metric {
    public double value;
    public LinkedMetric previous;

    @Override
    public void setValue(double value) {
      this.value = value;
    }

    @Override
    public Metric merge(Metric other) {
      this.value += ((LinkedMetric) other).value;
      return this;
    }

    @Override
    public double calculate() {
      return value;
    }
  }

  private static byte[] serialize(Object value) {
    Kryo kryo = new Kryo();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeClassAndObject(output, value);
    output.close();
    return baos.toByteArray();
  }

  @Test
  public void testAlgoMetricRoundTrip() {
    LossMetric loss = new LossMetric(4);
    loss.setValue(2.0);
    Metric lossCopy = KryoUtils.deserializeAlgoMetric(KryoUtils.serializeAlgoMetric(loss));
    assertEquals(LossMetric.class, lossCopy.getClass());
    assertEquals(4, ((LossMetric) lossCopy).sampleNum());
    assertEquals(0.5, lossCopy.calculate(), 0.0);

    ErrorMetric error = new ErrorMetric(10);
    error.setValue(0.25);
    Metric errorCopy = KryoUtils.deserializeAlgoMetric(KryoUtils.serializeAlgoMetric(error));
    assertEquals(ErrorMetric.class, errorCopy.getClass());
    assertEquals(10, ((ErrorMetric) errorCopy).sampleNum());
    assertEquals(0.25, errorCopy.calculate(), 0.0);

    ObjMetric obj = new ObjMetric();
    obj.setValue(-3.5);
    Metric objCopy = KryoUtils.deserializeAlgoMetric(KryoUtils.serializeAlgoMetric(obj));
    assertEquals(ObjMetric.class, objCopy.getClass());
    assertEquals(-3.5, objCopy.calculate(), 0.0);
  }

  @Test
  public void testAlgoMetricReferencingItsOwnClass() {
    LinkedMetric previous = new LinkedMetric();
    previous.setValue(1.0);
    LinkedMetric current = new LinkedMetric();
    current.setValue(2.0);
    current.previous = previous;

    // The class name of the nested metric is written as a reference to the outer one, so it can
    // only be read if the graph state is kept after the outer class is read. Read twice to make
    // sure the state is cleared again afterwards.
    for (int i = 0; i < 2; i++) {
      Metric copy = KryoUtils.deserializeAlgoMetric(KryoUtils.serializeAlgoMetric(current));
      assertEquals(LinkedMetric.class, copy.getClass());
      assertEquals(2.0, copy.calculate(), 0.0);
      assertEquals(1.0, ((LinkedMetric) copy).previous.calculate(), 0.0);
      assertNull(((LinkedMetric) copy).previous.previous);
    }
  }

  @Test
  public void testNullAlgoMetric() {
    assertNull(KryoUtils.deserializeAlgoMetric(KryoUtils.serializeAlgoMetric(null)));
  }

  @Test(expected = KryoException.class)
  public void testRejectNonMetricObject() {
    NotAMetric value = new NotAMetric();
    value.marker = "not a metric";
    value.payload = 1;
    KryoUtils.deserializeAlgoMetric(serialize(value));
  }

  @Test(expected = KryoException.class)
  public void testRejectNonMetricArrayBeforeAllocation() {
    // A long[] header with a forged length: without the type check the array would be allocated
    // before any element is read
    Kryo kryo = new Kryo();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeClass(output, long[].class);
    output.writeVarInt(Kryo.NOT_NULL, true);
    output.writeVarInt(Integer.MAX_VALUE, true);
    output.close();
    KryoUtils.deserializeAlgoMetric(baos.toByteArray());
  }
}
