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

package com.tencent.angel.ml.metric;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MultiErrorEvalMetricTest {
  private static final Log LOG = LogFactory.getLog(MultiErrorEvalMetricTest.class);
  private MultiErrorMetric multiErrorMetric = new MultiErrorMetric();
  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Test
  public void testEval() throws Exception {
    float pred[] = {0f, 1f};
    float label[] = {1f, 0f};
    assertEquals(8388608, multiErrorMetric.eval(pred, label), 0.00);
  }

  @Test
  public void testEvalOne() throws Exception {
    float pred[] = {0.6f, 0.3f, 0.7f};
    float label = 1f;
    assertEquals(1, multiErrorMetric.evalOne(pred, label), 0.0001);
    pred[1] = 1f;
    assertEquals(0, multiErrorMetric.evalOne(pred, label), 0.0001);
  }
}
