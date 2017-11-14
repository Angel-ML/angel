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

package com.tencent.angel.ml.optimizer.sgd;

import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.optimizer.sgd.loss.L2LogLoss;
import com.tencent.angel.ml.utils.MathsTest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class L2LogLossTest {
  private static final Log LOG = LogFactory.getLog(MathsTest.class);
  L2LogLoss l2LogLoss = new L2LogLoss(0.01);
  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testLoss() throws Exception {
    double data1[] = {1.0, 2.0};
    DenseDoubleVector denseDoubleVector1 = new DenseDoubleVector(2, data1);
    double data2[] = {1.0, 2.0};
    DenseDoubleVector denseDoubleVector2 = new DenseDoubleVector(2, data2);
    double test = l2LogLoss.loss(denseDoubleVector1, 2, denseDoubleVector2);
    assertEquals(Math.log(1 + Math.exp(-5 * 2)), test, 0.00);
  }


  @Test
  public void testLoss1() throws Exception {
    double pre = 2, y = 5;
    assertEquals(Math.log(1 + Math.exp(-2 * 5)), l2LogLoss.loss(pre, y), 0.00);
    y = 10;
    assertEquals(Math.exp(-20), l2LogLoss.loss(pre, y), 0.00);
    y = -10;
    assertEquals(20, l2LogLoss.loss(pre, y), 0.00);

  }

  @Test
  public void testLoss2() throws Exception {
    double data1[] = {1.0, 2.0};
    double data2[] = {2.0, 1.0};
    DenseDoubleVector denseDoubleVector1 = new DenseDoubleVector(2, data1);
    DenseDoubleVector denseDoubleVector2 = new DenseDoubleVector(2, data1);
    DenseDoubleVector w = new DenseDoubleVector(2, data2);
    TAbstractVector[] xList = new TAbstractVector[2];
    xList[0] = denseDoubleVector1;
    xList[1] = denseDoubleVector2;
    double[] yList = new double[2];
    yList[0] = 0;
    yList[1] = 1;
    double test = l2LogLoss.loss(xList, yList, w, 2);
    assertEquals(0.736297, test, 0.00001);
  }

  @Test
  public void testGrad() throws Exception {
    double pre = 2, y = 5;
    double test = l2LogLoss.grad(pre, y);
    assertEquals((5 / (1.0 + Math.exp(10))), test, 0.00001);
  }

  @Test
  public void testPredict() throws Exception {

  }
}
