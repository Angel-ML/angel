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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MathUtilsTest {
  private static final Log LOG = LogFactory.getLog(MathsTest.class);

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Test public void testSigmoid() throws Exception {
    float data[] = {0.000001f, 1f, 100f, 1000f};
    for (int i = 0; i < data.length; i++) {
      // LOG.info(sigmoid(data[i]));
      // LOG.info(Math.exp(-data[i]));
      assertTrue((Maths.sigmoid(data[i]) > 0.00f) && (Maths.sigmoid(data[i]) <= 1.00f));
    }
  }

  @Test public void testSigmoid1() throws Exception {
    double data[] = {0.000001, 1, 100, 1000};
    for (int i = 0; i < data.length; i++) {
      assertTrue((Maths.sigmoid(data[i]) > 0.00) && (Maths.sigmoid(data[i]) <= 1.00));
    }
  }

  @Test public void testSoftmax() throws Exception {
    double data[] = {0.2, 3, 4, 900, 1};
    Maths.softmax(data);
    for (int i = 0; i < data.length; i++) {
      assertTrue((Maths.sigmoid(data[i]) > 0.00) && (Maths.sigmoid(data[i]) <= 1.00));
    }

  }

  @Test public void testSoftmax1() throws Exception {
    float data[] = {0.2f, 3f, 4f, 900f, 1f};
    Maths.softmax(data);
    for (int i = 0; i < data.length; i++) {
      assertTrue((Maths.sigmoid(data[i]) > 0.00f) && (Maths.sigmoid(data[i]) <= 1.00f));
    }
  }

  @Test public void testThresholdL1() throws Exception {
    double data[] = {0.2, 3, -4, 900, -200};
    assertTrue(Maths.thresholdL1(data[0], 20) == 0);
    assertTrue(Maths.thresholdL1(data[1], 20) == 0);
    assertTrue(Maths.thresholdL1(data[2], 20) == 0);
    assertTrue(Maths.thresholdL1(data[3], 20) == 880.0);
    assertTrue(Maths.thresholdL1(data[4], 20) == -180.0);
  }

  @Test public void testThresholdL11() throws Exception {
    float data[] = {0.2f, 3f, -4f, 900f, -200f};
    assertTrue(Maths.thresholdL1(data[0], 20) == 0f);
    assertTrue(Maths.thresholdL1(data[1], 20) == 0f);
    assertTrue(Maths.thresholdL1(data[2], 20) == 0f);
    assertTrue(Maths.thresholdL1(data[3], 20) == 880.0f);
    assertTrue(Maths.thresholdL1(data[4], 20) == -180.0f);
  }

  @Test public void testIsEven() throws Exception {
    int data1[] = {2, 4, 6, 8, 10};
    for (int i = 0; i < data1.length; i++)
      assertTrue(Maths.isEven(data1[i]));
    int data2[] = {1, 3, 5, 7, 9};
    for (int i = 0; i < data2.length; i++)
      assertFalse(Maths.isEven(data2[i]));
  }

  @Test public void testPow() throws Exception {
    int a[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < a.length; i++)
      assertTrue(Maths.pow(a[i], 0) == 1);
    for (int i = 0; i < a.length; i++)
      assertTrue(Maths.pow(a[i], 1) == a[i]);
    for (int i = 0; i < a.length; i++)
      assertTrue(Maths.pow(a[i], 2) == a[i] * a[i]);
    for (int i = 0; i < a.length; i++)
      assertTrue(Maths.pow(a[i], 3) == a[i] * a[i] * a[i]);
    // when b is a negative number
    for (int i = 0; i < a.length; i++)
      assertTrue(Maths.pow(a[i], -2) == a[i] * a[i]);
  }

  @Test public void testShuffle() throws Exception {

  }

  @Test public void testIntList2Arr() throws Exception {
    int data[] = {-1, 2, 3, 900};
    List<Integer> integers = new ArrayList();
    for (int i = 0; i < data.length; i++)
      integers.add(i, data[i]);
    int test[] = Maths.intList2Arr(integers);
    assertArrayEquals(data, test);
  }

  @Test public void testFloatList2Arr() throws Exception {
    float data[] = {-1f, 2f, 3f, 900f};
    List<Float> floats = new ArrayList();
    for (int i = 0; i < data.length; i++)
      floats.add(i, data[i]);
    float test[] = Maths.floatList2Arr(floats);
    for (int i = 0; i < data.length; i++)
      assertEquals(data[i], test[i], 0.0f);
  }

  @Test public void testList2Arr() throws Exception {

  }

  @Test public void testFindMaxIndex() throws Exception {
    float data[] = {-1f, 2f, 3f, 900f};
    int index = Maths.findMaxIndex(data);
    assertEquals(index, 3);
  }

  @Test public void testDouble2Float() throws Exception {
    float data[] = {-1.00f, 2.00f, 3.00f, 900.00f};
    double test[] = {-1, 2, 3, 900};
    float dataTest[] = Maths.double2Float(test);
    for (int i = 0; i < data.length; i++)
      assertEquals(data[i], dataTest[i], 0.0f);
  }

  @Test public void testMain() throws Exception {

  }
}
