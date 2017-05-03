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
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.mockito.Mockito.when;

/**
 * DiskStorage Tester.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(WorkerContext.class)
public class DiskStorageTest {

  private DiskStorage<Integer> diskStorage;
  @Mock
  private WorkerContext workerContext;

  @Mock
  private WorkerAttemptId workerAttemptId;

  @Mock
  private ApplicationId applicationId;

  private Configuration conf;
  private static int TASK_ID = 127;

  @BeforeClass
  public static void setUp(){
    System.setProperty("HADOOP_USER_NAME",System.getProperty("user.name"));
  }

  @Before
  public void before() throws Exception {
    PowerMockito.mockStatic(WorkerContext.class);
    PowerMockito.when(WorkerContext.get()).thenReturn(workerContext);
    when(workerAttemptId.toString()).thenReturn("worker-appempt001");
    when(applicationId.toString()).thenReturn("application_01_01");
    when(workerContext.getAppId()).thenReturn(applicationId);
    when(workerContext.getWorkerAttemptId()).thenReturn(workerAttemptId);
    when(workerContext.getUser()).thenReturn(UserGroupInformation.getCurrentUser().getShortUserName());
    conf = new Configuration();
    conf.set(AngelConfiguration.LOCAL_DIR,System.getProperty("java.io.tmpdir","/tmp"));
    conf.setInt(AngelConfiguration.ANGEL_TASK_RECORD_FILE_MAXSIZE_MB, 1);
    when(workerContext.getConf()).thenReturn(conf);
    diskStorage = new DiskStorage<Integer>(TASK_ID);
  }




  @Test
  public void testPutData() throws Exception {
    int maxNum = (1 << 20) / 4;
    for (int i = 0; i < maxNum * 2; i++) {
      diskStorage.put(i);
    }
    diskStorage.flush();
    Assert.assertTrue(diskStorage.getTotalElemNum() == maxNum * 2);

  }

  @Test
  public void testReadData() throws Exception {
    diskStorage.registerType(Integer.class);
    int maxNum = 1024;
    for (int i = 0; i< maxNum;i++ ) {
      diskStorage.put(i);
    }
    diskStorage.flush();
    int count = 0;
    while (diskStorage.hasNext()) {
      Assert.assertTrue(diskStorage.read()== count++);
    }
    Assert.assertTrue(count == maxNum);

    diskStorage.resetReadIndex();
    count = 0;
    while (diskStorage.hasNext()) {
      Assert.assertTrue(diskStorage.read()== count++);
    }
    Assert.assertTrue(count == maxNum);
  }

  @Test
  public void testClean() throws IOException {
    diskStorage.registerType(Integer.class);
    int maxNum = 1024;
    for (int i = 0; i< maxNum;i++ ) {
      diskStorage.put(i);
    }
    diskStorage.flush();
    Assert.assertTrue(diskStorage.hasNext());
    diskStorage.clean();
    Assert.assertTrue(!diskStorage.hasNext());

  }




}
