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

package com.tencent.angel.ps;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class PSAttemptIdTest {
  final private int psIndex = 0;
  private PSAttemptId psAttemptId;
  private ParameterServerId psId;

  @Before
  public void setUp() throws Exception {
    psId = new ParameterServerId(psIndex);
    psAttemptId = new PSAttemptId(psId, 1);

  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testGetParameterServerId() throws Exception {
    assertEquals(psId, psAttemptId.getParameterServerId());
  }

  @Test
  public void testAppendTo() throws Exception {

  }

  @Test
  public void testToString() throws Exception {
    assertEquals("PSAttempt_0_1", psAttemptId.toString());
  }

  @Test
  public void testHashCode() throws Exception {
    assertEquals(31, psAttemptId.hashCode());

  }

  @Test
  public void testEquals() throws Exception {
    ParameterServerId psIdNew = new ParameterServerId(psIndex);
    PSAttemptId psAttemptIdNew = new PSAttemptId(psIdNew, 1);
    assertEquals(true, psAttemptId.equals(psAttemptIdNew));
  }
}


