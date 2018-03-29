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

import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.optimizer.sgd.loss.SquareL2Loss;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SquareLossTest {
  private static final Log LOG = LogFactory.getLog(SquareLossTest.class);

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before public void setUp() throws Exception {

  }

  @Test public void testLoss() throws Exception {
    double dot = 1.0, y = 2.0;
    SquareL2Loss squareLoss = new SquareL2Loss();
    double test = squareLoss.loss(dot, y);
    assertEquals(0.50, test, 0.00);
  }

  @Test public void testGrad() throws Exception {
    double dot = 1.0, y = 2.0;
    SquareL2Loss squareLoss = new SquareL2Loss();
    double test = squareLoss.grad(dot, y);
    assertEquals(1.0, test, 0.00);
  }

  @Test public void testPredict() throws Exception {
    double data1[] = {1.0, 2.0, 3.0, 4.0};
    DenseDoubleVector denseDoubleVector1 = new DenseDoubleVector(4, data1);
    double data2[] = {1.0, 2.0, 3.0, 4.0};
    DenseDoubleVector denseDoubleVector2 = new DenseDoubleVector(4, data2);
    SquareL2Loss squareLoss = new SquareL2Loss();
    double test = squareLoss.predict(denseDoubleVector1, denseDoubleVector2);
    double dot = 0.0;
    for (int i = 0; i < data1.length; i++)
      dot += data1[i] * data2[i];
    assertEquals(dot, test, 0.00);
  }
}
