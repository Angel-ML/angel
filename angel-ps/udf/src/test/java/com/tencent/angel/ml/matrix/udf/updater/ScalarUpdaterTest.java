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

package com.tencent.angel.ml.matrix.udf.updater;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMetaManager;
import com.tencent.angel.ml.matrix.udf.TestUDFUtils;
import com.tencent.angel.ps.impl.MatrixPartitionManager;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseIntRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.psagent.MatrixPartitionRouter;
import com.tencent.angel.psagent.PSAgentContext;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.powermock.api.mockito.PowerMockito.*;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

/**
 * ScalarUpdater Tester.
 */
@RunWith(PowerMockRunner.class) @PrepareForTest({PSAgentContext.class, PSContext.class})
public class ScalarUpdaterTest {

  private ScalarUpdater updater;

  private ScalarUpdater.ScalarUpdaterParam updaterParam;

  @Mock
  private PSAgentContext psAgentContext;

  @Mock
  private PSContext psContext;

  @Mock
  private MatrixPartitionRouter matrixPartitionRouter;

  @Mock
  private MatrixMetaManager matrixMetaManager;

  @Mock
  private MatrixPartitionManager matrixPartitionManager;

  private final static int MATRIX_ID = 127;
  private PartitionKey partitionKey0;
  private PartitionKey partitionKey1;

  @BeforeClass
  public static void beforeClass() {
    mockStatic(PSAgentContext.class);
    mockStatic(PSContext.class);
  }

  @Before
  public void before() throws Exception {
    when(PSAgentContext.get()).thenReturn(psAgentContext);
    when(PSContext.get()).thenReturn(psContext);
    updaterParam = new ScalarUpdater.ScalarUpdaterParam(MATRIX_ID,true,0.2D);
    updater = new ScalarUpdater(updaterParam);
    partitionKey0 = new PartitionKey(0, MATRIX_ID, 0, 0, 1, 100);
    partitionKey1 = new PartitionKey(1, MATRIX_ID, 0, 100, 1, 200);
    List<PartitionKey> partitionKeys = Arrays.asList(partitionKey0,
        partitionKey1);
    when(psAgentContext.getMatrixPartitionRouter()).thenReturn(matrixPartitionRouter);
    when(matrixPartitionRouter.getPartitionKeyList(MATRIX_ID)).thenReturn(partitionKeys);
    when(psContext.getMatrixPartitionManager()).thenReturn(matrixPartitionManager);
  }

  @Test
  public void testUpdate() {
    updaterParam = new ScalarUpdater.ScalarUpdaterParam(MATRIX_ID, true, 2D);
    updater = new ScalarUpdater(updaterParam);
    ServerPartition partition1 = mock(ServerPartition.class);
    ServerPartition partition0 = mock(ServerPartition.class);
    when(matrixPartitionManager.getPartition(MATRIX_ID, 0)).thenReturn(partition0);
    when(matrixPartitionManager.getPartition(MATRIX_ID, 1)).thenReturn(partition1);

    when(partition0.getPartitionKey()).thenReturn(partitionKey0);
    when(partition1.getPartitionKey()).thenReturn(partitionKey1);
    byte[] data0 = new byte[100 * 4];
    TestUDFUtils.fillInt(data0, 2);
    byte[] data1 = new byte[100 * 4];
    TestUDFUtils.fillInt(data1, 6);
    when(partition0.getRow(0)).thenReturn(new ServerDenseIntRow(0, 0, 100, data0));
    when(partition1.getRow(0)).thenReturn(new ServerDenseIntRow(0, 100, 200, data1));
    for (PartitionUpdaterParam param : updater.getParam().split()) {
      updater.partitionUpdate(param);
    }
    Assert.assertTrue(
        TestUDFUtils.bytes2Int(((ServerDenseIntRow) partition0.getRow(0)).getDataArray(), 0) == 4);
    Assert.assertTrue(
        TestUDFUtils.bytes2Int(((ServerDenseIntRow) partition1.getRow(0)).getDataArray(), 4) == 12);
  }


} 
