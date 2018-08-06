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

package com.tencent.angel.ps;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class ParameterServerIdTest {

  final private int psIndex = 0;
  private ParameterServerId psId;

  @Before
  public void setUp() throws Exception {
    psId = new ParameterServerId(psIndex);
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testAppendTo() throws Exception {}

  @Test
  public void testToString() throws Exception {
    assertEquals("ParameterServer_0", psId.toString());

  }
}
