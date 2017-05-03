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

package com.tencent.angel.ml.matrix.udf.getrow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.MatrixMetaManager;
import com.tencent.angel.ml.matrix.udf.TestUDFUtils;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.ps.impl.MatrixPartitionManager;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseIntRow;
import com.tencent.angel.psagent.MatrixPartitionRouter;
import com.tencent.angel.psagent.PSAgentContext;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Before; 
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import static  org.powermock.api.mockito.PowerMockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** 
* DefaultGetRowFunc Tester.
*/
@RunWith(PowerMockRunner.class)
@PrepareForTest({PSAgentContext.class,PSContext.class})
public class DefaultGetRowFuncTest {

  private GetRowFunc getRowFunc;

  private GetRowParam getRowParam;

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

  private final static int ROW_INDEX = 1;

  private final static int CLOCK = 1;

  private List<PartitionKey> partitionKeys;

  @BeforeClass
  public static void beforeClass(){
    mockStatic(PSAgentContext.class);
    mockStatic(PSContext.class);
  }

  @Before
  public void before() throws Exception {
    when(PSAgentContext.get()).thenReturn(psAgentContext);
    when(PSContext.get()).thenReturn(psContext);
    partitionKeys = Arrays.asList(new PartitionKey(0, MATRIX_ID, 0, 0, 100, 100),
        new PartitionKey(1, MATRIX_ID, 0, 100, 100, 200));
    when(psAgentContext.getMatrixPartitionRouter()).thenReturn(matrixPartitionRouter);
    when(psAgentContext.getMatrixMetaManager()).thenReturn(matrixMetaManager);
    when(psContext.getMatrixPartitionManager()).thenReturn(matrixPartitionManager);
    when(matrixMetaManager.getMatrixMeta(MATRIX_ID)).thenReturn(new MatrixMeta(
        MLProtos.MatrixProto.newBuilder().setId(MATRIX_ID).setName("matrix").setRowNum(100)
            .setColNum(200).setRowType(MLProtos.RowType.T_INT_DENSE).build()));
  }



  @Test
  public void testGetRow() throws Exception {
    getRowParam = new GetRowParam(MATRIX_ID, ROW_INDEX, CLOCK, true);
    getRowFunc = new DefaultGetRowFunc(getRowParam);
    //mock split
    when(matrixPartitionRouter.getPartitionKeyList(MATRIX_ID)).thenReturn(partitionKeys);

    //mock partition get
    byte[] partition1Data = new byte[100 * 4];
    TestUDFUtils.fillInt(partition1Data, 127);
    byte[] partition2Data = new byte[100 * 4];
    TestUDFUtils.fillInt(partition2Data, 128);

    when(matrixPartitionManager.getRow(MATRIX_ID, ROW_INDEX, 0))
        .thenReturn(new ServerDenseIntRow(ROW_INDEX, 0, 100, partition1Data));
    when(matrixPartitionManager.getRow(MATRIX_ID, ROW_INDEX, 1))
        .thenReturn(new ServerDenseIntRow(ROW_INDEX, 100, 200, partition2Data));

    List<PartitionGetRowResult> results = new ArrayList<PartitionGetRowResult>();
    for (PartitionGetRowParam param : getRowFunc.getParam().split()) {
      results.add(getRowFunc.partitionGet(param));
    }
    GetRowResult result = getRowFunc.merge(results);
    Assert.assertTrue(((DenseIntVector) result.getRow()).getValues().length == 200);
    Assert.assertTrue(((DenseIntVector) result.getRow()).getValues()[0]==127);
    Assert.assertTrue(((DenseIntVector) result.getRow()).getValues()[100]==128);

  }





} 
