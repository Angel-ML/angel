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

package com.tencent.angel.client.standalone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.client.embedded.AngelEmbeddedClient;
import com.tencent.angel.client.local.AngelLocalClient;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.deploy.ContainerAllocatorFactory;
import com.tencent.angel.master.deploy.ContainerLauncherFactory;
import com.tencent.angel.master.deploy.DeployUtils;
import com.tencent.angel.master.deploy.standalone.StandaloneContainerAllocator;
import com.tencent.angel.master.deploy.standalone.StandaloneContainerLauncher;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Unit-level tests for the STANDALONE deploy mode wiring:
 *   - Enum and {@link DeployUtils#getDeployMode(Configuration)} resolution
 *   - {@link AngelClientFactory} returns {@link AngelStandaloneClient}
 *   - {@link ContainerAllocatorFactory}/{@link ContainerLauncherFactory} return
 *     standalone allocator/launcher
 *
 * These tests do NOT spawn any external process and run in milliseconds.
 */
public class StandaloneClientFactoryTest {

  @Test public void testStandaloneEnumPresent() {
    AngelDeployMode mode = AngelDeployMode.valueOf("STANDALONE");
    assertEquals(AngelDeployMode.STANDALONE, mode);
  }

  @Test public void testDeployUtilsResolveStandalone() {
    Configuration conf = new Configuration();
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "STANDALONE");
    assertEquals(AngelDeployMode.STANDALONE, DeployUtils.getDeployMode(conf));
  }

  @Test public void testDeployUtilsResolveLocalUnchanged() {
    Configuration conf = new Configuration();
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
    assertEquals(AngelDeployMode.LOCAL, DeployUtils.getDeployMode(conf));
  }

  @Test public void testDeployUtilsResolveEmbeddedUnchanged() {
    Configuration conf = new Configuration();
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "EMBEDDED");
    assertEquals(AngelDeployMode.EMBEDDED, DeployUtils.getDeployMode(conf));
  }

  @Test public void testDeployUtilsResolveDefaultYarn() {
    Configuration conf = new Configuration();
    // do not set ANGEL_DEPLOY_MODE
    assertEquals(AngelDeployMode.YARN, DeployUtils.getDeployMode(conf));
  }

  @Test public void testClientFactoryReturnsStandaloneClient() {
    Configuration conf = new Configuration();
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "STANDALONE");
    AngelClient client = AngelClientFactory.get(conf);
    assertNotNull(client);
    assertTrue("AngelClientFactory should return AngelStandaloneClient for STANDALONE mode, got "
        + client.getClass(), client instanceof AngelStandaloneClient);
  }

  @Test public void testClientFactoryRoutingForOtherModesUnchanged() {
    Configuration localConf = new Configuration();
    localConf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
    assertTrue(AngelClientFactory.get(localConf) instanceof AngelLocalClient);

    Configuration embeddedConf = new Configuration();
    embeddedConf.set(AngelConf.ANGEL_DEPLOY_MODE, "EMBEDDED");
    assertTrue(AngelClientFactory.get(embeddedConf) instanceof AngelEmbeddedClient);
  }

  @Test public void testContainerAllocatorFactory() {
    // AMContext is null on purpose; both standalone allocator/launcher accept a null context
    // for ctor wiring. They never dereference context until handle() is invoked.
    assertTrue(
        ContainerAllocatorFactory.create(AngelDeployMode.STANDALONE, null)
            instanceof StandaloneContainerAllocator);
  }

  @Test public void testContainerLauncherFactory() {
    assertTrue(
        ContainerLauncherFactory.create(AngelDeployMode.STANDALONE, null)
            instanceof StandaloneContainerLauncher);
  }
}
