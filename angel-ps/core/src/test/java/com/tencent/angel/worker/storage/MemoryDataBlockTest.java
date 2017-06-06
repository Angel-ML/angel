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

package com.tencent.angel.worker.storage;

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.mockito.Mockito.when;

/**
 * MemoryDataBlock Tester.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(WorkerContext.class)
public class MemoryDataBlockTest {

  private MemoryDataBlock<Integer> memoryStorage;
  private WorkerContext workerContext;

  @Before
  public void before () throws Exception {
    workerContext = Mockito.mock(WorkerContext.class);
    PowerMockito.mockStatic(WorkerContext.class);
    PowerMockito.when(WorkerContext.get()).thenReturn(workerContext);
    Configuration conf = new Configuration();
    when(workerContext.getConf()).thenReturn(conf);
    memoryStorage = new MemoryDataBlock<Integer>(1025);
    for (int i = 0; i < 1024; i++) {
      memoryStorage.put(i);
    }
  }


  @Test
  public void testPutData() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(AngelConfiguration.ANGEL_TASK_MEMORYSTORAGE_USE_MAX_MEMORY_MB, 1);
    when(workerContext.getConf()).thenReturn(conf);
    memoryStorage = new MemoryDataBlock<Integer>(0);
    for (int i = 0; i < 1024; i++) {
      memoryStorage.put(i);
    }
    Assert.assertTrue(memoryStorage.getvList().size() == 1024);
    Assert.assertTrue(!memoryStorage.checkIsOverMaxMemoryUsed());
  }

  @Test
  public void testOverMemoryUsed() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(AngelConfiguration.ANGEL_TASK_MEMORYSTORAGE_USE_MAX_MEMORY_MB, 1);
    when(workerContext.getConf()).thenReturn(conf);
    memoryStorage = new MemoryDataBlock<Integer>(0);
    int estimizedIntNum = (1 << 20) / (Integer.SIZE / 8);
    for (int i = 0; i < estimizedIntNum - 1; i++) {
      memoryStorage.put(i);
    }
    Assert.assertTrue(!memoryStorage.checkIsOverMaxMemoryUsed());
    memoryStorage.put(estimizedIntNum - 1);
    Assert.assertTrue(memoryStorage.checkIsOverMaxMemoryUsed());

  }

  @Test
  public void testReadData() throws Exception {
    int count = 0;
    while (memoryStorage.hasNext()) {
      count++;
      Assert.assertTrue(memoryStorage.read() != null);
    }
    Assert.assertTrue(count == 1024);
  }

  @Test
  public void testSlice() throws Exception {
    DataBlock<Integer> slice = memoryStorage.slice(256, 256);
    int count = 0;
    while (slice.hasNext()) {
      count++;
      slice.read();
    }
    Assert.assertTrue(count == 256);
  }

}
