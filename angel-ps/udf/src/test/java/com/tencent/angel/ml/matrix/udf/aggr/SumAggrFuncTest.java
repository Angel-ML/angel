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

package com.tencent.angel.ml.matrix.udf.aggr;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMetaManager;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.ps.impl.MatrixPartitionManager;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.psagent.MatrixPartitionRouter;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;

/**
 * SumAggrFunc Tester.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PSAgentContext.class,PSContext.class})
public class SumAggrFuncTest {
  private SumAggrFunc sumAggrFunc;

  private SumAggrFunc.SumAggrParam sumAggrParam;

  @Mock
  private MatrixMetaManager matrixMetaManager;


  @Mock
  private MatrixPartitionManager matrixPartitionManager;

  @Mock
  private MatrixPartitionRouter matrixLocationRouter;

  @Mock
  private PSAgentContext psAgentContext ;

  @Mock
  private PSContext psContext;

  private static final int METRIC_ID = 128;
  public static final PartitionKey PARTITION_KEY_1 = new PartitionKey(1, METRIC_ID, 0, 0, 100, 100);

  public static final PartitionKey PARTITION_KEY_2 = new PartitionKey(2, METRIC_ID, 0, 0, 100, 100);


  @Before
  public void before() throws Exception {
    sumAggrParam = new SumAggrFunc.SumAggrParam(METRIC_ID);
    sumAggrFunc = new SumAggrFunc(sumAggrParam);



    PowerMockito.mockStatic(PSAgentContext.class);
    PowerMockito.when(PSAgentContext.get()).thenReturn(psAgentContext);
    PowerMockito.mockStatic(PSContext.class);
    PowerMockito.when(PSContext.get()).thenReturn(psContext);


    PowerMockito.when(psAgentContext.getMatrixMetaManager()).thenReturn(matrixMetaManager);
    PowerMockito.when(psAgentContext.getMatrixPartitionRouter()).thenReturn(matrixLocationRouter);
    PowerMockito.when(psContext.getMatrixPartitionManager()).thenReturn(matrixPartitionManager);
    when(matrixLocationRouter.getPartitionKeyList(METRIC_ID)).thenReturn(Arrays.asList(PARTITION_KEY_1,
        PARTITION_KEY_2));


  }



  @Test
  public void testAggr() throws Exception {
    ServerPartition serverPartition = Mockito.mock(ServerPartition.class);
    when(serverPartition.getPartitionKey()).thenReturn(PARTITION_KEY_1);
    when(matrixPartitionManager.getPartition(METRIC_ID, PARTITION_KEY_1.getPartitionId()))
        .thenReturn(serverPartition);


    // SparseIntRow
    for (int i = 0; i < 100; i++) {
      ServerSparseIntRow serverRow = new ServerSparseIntRow(i, 0, 100);
      // dense
      ByteBuf byteBuf = Mockito.mock(ByteBuf.class);
      OngoingStubbing<Integer> chain = null;
      OngoingStubbing<Integer> valueChaine = null;
      for (int updateIdx = 0; updateIdx < 100; updateIdx++) {
        if (chain == null) {
          chain = when(byteBuf.readInt());
        }
        chain = chain.thenReturn(updateIdx);

      }
      serverRow.update(MLProtos.RowType.T_INT_DENSE, byteBuf, 100);

      // sparse
      byteBuf = Mockito.mock(ByteBuf.class);
      ByteBuf valueBuf = Mockito.mock(ByteBuf.class);
      when(byteBuf.slice(4 * 100, 4 * 100)).thenReturn(valueBuf);
      chain = null;
      valueChaine = null;


      for (int updateIdx = 0; updateIdx < 100; updateIdx++) {
        if (chain == null) {
          chain = when(byteBuf.readInt());
        }
        chain = chain.thenReturn(updateIdx);

        if (valueChaine == null) {
          valueChaine = when(valueBuf.readInt());
        }
        valueChaine = valueChaine.thenReturn(updateIdx);

      }
      serverRow.update(MLProtos.RowType.T_INT_SPARSE, byteBuf, 100);

      when(serverPartition.getRow(i)).thenReturn(serverRow);
    }
    PartitionAggrResult partitionAggrResult =
        sumAggrFunc.aggr(new PartitionAggrParam(METRIC_ID, PARTITION_KEY_1));
    Assert.assertTrue(partitionAggrResult != null);
    Assert.assertTrue(
        ((SumAggrFunc.SumAggrResult) sumAggrFunc.merge(Arrays.asList(partitionAggrResult)))
            .getResult() == 990000D);



    /*--------------------------------------*/
    //DenseIntRow
    for (int i = 0; i < 100; i++) {
      ServerDenseIntRow serverRow = new ServerDenseIntRow(i, 0, 100);
      // dense
      ByteBuf byteBuf = Mockito.mock(ByteBuf.class);
      OngoingStubbing<Integer> loop = null;
      OngoingStubbing<Integer> valueLoop = null;
      for (int updateIdx = 0; updateIdx < 100; updateIdx++) {
        if (loop == null) {
          loop = when(byteBuf.readInt());
        }
        loop = loop.thenReturn(updateIdx);

      }
      serverRow.update(MLProtos.RowType.T_INT_DENSE, byteBuf, 100);

      // sparse
      byteBuf = Mockito.mock(ByteBuf.class);
      ByteBuf valueBuf = Mockito.mock(ByteBuf.class);
      when(byteBuf.slice(4 * 100, 4 * 100)).thenReturn(valueBuf);
      loop = null;
      valueLoop = null;


      for (int updateIdx = 0; updateIdx < 100; updateIdx++) {
        if (loop == null) {
          loop = when(byteBuf.readInt());
        }
        loop = loop.thenReturn(updateIdx);

        if (valueLoop == null) {
          valueLoop = when(valueBuf.readInt());
        }
        valueLoop = valueLoop.thenReturn(updateIdx);

      }
      serverRow.update(MLProtos.RowType.T_INT_SPARSE, byteBuf, 100);

      when(serverPartition.getRow(i)).thenReturn(serverRow);
    }
    partitionAggrResult = sumAggrFunc.aggr(new PartitionAggrParam(METRIC_ID, PARTITION_KEY_1));
    Assert.assertTrue(partitionAggrResult != null);
    Assert.assertTrue(
        ((SumAggrFunc.SumAggrResult) sumAggrFunc.merge(Arrays.asList(partitionAggrResult)))
            .getResult() == 990000D);



    /*--------------------------------------*/
    //SparseDoubleRow
    for (int i = 0; i < 100; i++) {
      ServerSparseDoubleRow serverRow = new ServerSparseDoubleRow(i, 0, 100);


      // dense
      ByteBuf byteBuf = Mockito.mock(ByteBuf.class);
      OngoingStubbing<Double> loop = null;
      OngoingStubbing<Double> valueLoop = null;
      for (int updateIdx = 0; updateIdx < 100; updateIdx++) {
        if (loop == null) {
          loop = when(byteBuf.readDouble());
        }
        loop = loop.thenReturn(updateIdx * 1.0D);

      }
      serverRow.update(MLProtos.RowType.T_DOUBLE_DENSE, byteBuf, 100);



      // sparse
      byteBuf = Mockito.mock(ByteBuf.class);
      ByteBuf valueBuf = Mockito.mock(ByteBuf.class);
      when(byteBuf.slice(4 * 100, 8 * 100)).thenReturn(valueBuf);
      valueLoop = null;
      OngoingStubbing<Integer> keyLoop = null;
      for (int updateIdx = 0; updateIdx < 100; updateIdx++) {
        if (keyLoop == null) {
          keyLoop = when(byteBuf.readInt());
        }
        keyLoop = keyLoop.thenReturn(updateIdx );

        if (valueLoop == null) {
          valueLoop = when(valueBuf.readDouble());
        }
        valueLoop = valueLoop.thenReturn(updateIdx * 1.0D);

      }
      serverRow.update(MLProtos.RowType.T_DOUBLE_SPARSE, byteBuf, 100);

      when(serverPartition.getRow(i)).thenReturn(serverRow);
    }
    partitionAggrResult = sumAggrFunc.aggr(new PartitionAggrParam(METRIC_ID, PARTITION_KEY_1));
    Assert.assertTrue(partitionAggrResult != null);
    Assert.assertTrue(
        ((SumAggrFunc.SumAggrResult) sumAggrFunc.merge(Arrays.asList(partitionAggrResult)))
            .getResult() == 990000D);


    /*--------------------------------------*/
    //DenseDoubleRow
    for (int i = 0; i < 100; i++) {
      ServerDenseDoubleRow serverRow = new ServerDenseDoubleRow(i, 0, 100);


      // dense
      ByteBuf byteBuf = Mockito.mock(ByteBuf.class);
      OngoingStubbing<Double> loop = null;
      OngoingStubbing<Double> valueLoop = null;
      for (int updateIdx = 0; updateIdx < 100; updateIdx++) {
        if (loop == null) {
          loop = when(byteBuf.readDouble());
        }
        loop = loop.thenReturn(updateIdx * 1.0D);

      }
      serverRow.update(MLProtos.RowType.T_DOUBLE_DENSE, byteBuf, 100);



      // sparse
      byteBuf = Mockito.mock(ByteBuf.class);
      ByteBuf valueBuf = Mockito.mock(ByteBuf.class);
      when(byteBuf.slice(4 * 100, 8 * 100)).thenReturn(valueBuf);

      valueLoop = null;

      OngoingStubbing<Integer> keyLoop = null;
      for (int updateIdx = 0; updateIdx < 100; updateIdx++) {
        if (keyLoop == null) {
          keyLoop = when(byteBuf.readInt());
        }
        keyLoop = keyLoop.thenReturn(updateIdx );

        if (valueLoop == null) {
          valueLoop = when(valueBuf.readDouble());
        }
        valueLoop = valueLoop.thenReturn(updateIdx * 1.0D);

      }
      serverRow.update(MLProtos.RowType.T_DOUBLE_SPARSE, byteBuf, 100);

      when(serverPartition.getRow(i)).thenReturn(serverRow);
    }
    partitionAggrResult = sumAggrFunc.aggr(new PartitionAggrParam(METRIC_ID, PARTITION_KEY_1));
    Assert.assertTrue(partitionAggrResult != null);
    Assert.assertTrue(
        ((SumAggrFunc.SumAggrResult) sumAggrFunc.merge(Arrays.asList(partitionAggrResult)))
            .getResult() == 990000D);



    /*--------------------------------------*/
    //SparseFloatRow
    for (int i = 0; i < 100; i++) {
      ServerSparseFloatRow serverRow = new ServerSparseFloatRow(i, 0, 100);


      // dense
      ByteBuf byteBuf = Mockito.mock(ByteBuf.class);
      OngoingStubbing<Float> loop = null;
      OngoingStubbing<Float> valueLoop = null;
      for (int updateIdx = 0; updateIdx < 100; updateIdx++) {
        if (loop == null) {
          loop = when(byteBuf.readFloat());
        }
        loop = loop.thenReturn(updateIdx * 1.0F);

      }
      serverRow.update(MLProtos.RowType.T_FLOAT_DENSE, byteBuf, 100);



      // sparse
      byteBuf = Mockito.mock(ByteBuf.class);
      ByteBuf valueBuf = Mockito.mock(ByteBuf.class);
      when(byteBuf.slice(4 * 100, 4 * 100)).thenReturn(valueBuf);
      valueLoop = null;

      OngoingStubbing<Integer> keyLoop = null;
      for (int updateIdx = 0; updateIdx < 100; updateIdx++) {
        if (keyLoop == null) {
          keyLoop = when(byteBuf.readInt());
        }
        keyLoop = keyLoop.thenReturn(updateIdx);

        if (valueLoop == null) {
          valueLoop = when(valueBuf.readFloat());
        }
        valueLoop = valueLoop.thenReturn(updateIdx * 1.0F);

      }
      serverRow.update(MLProtos.RowType.T_FLOAT_SPARSE, byteBuf, 100);

      when(serverPartition.getRow(i)).thenReturn(serverRow);
    }
    partitionAggrResult = sumAggrFunc.aggr(new PartitionAggrParam(METRIC_ID, PARTITION_KEY_1));
    Assert.assertTrue(partitionAggrResult != null);
    Assert.assertTrue(
        ((SumAggrFunc.SumAggrResult) sumAggrFunc.merge(Arrays.asList(partitionAggrResult)))
            .getResult() == 990000D);


    /*--------------------------------------*/
    //DenseFloatRow
    for (int i = 0; i < 100; i++) {
      ServerDenseFloatRow serverRow = new ServerDenseFloatRow(i, 0, 100);


      // dense
      ByteBuf byteBuf = Mockito.mock(ByteBuf.class);
      OngoingStubbing<Float> loop = null;
      OngoingStubbing<Float> valueLoop = null;
      for (int updateIdx = 0; updateIdx < 100; updateIdx++) {
        if (loop == null) {
          loop = when(byteBuf.readFloat());
        }
        loop = loop.thenReturn(updateIdx * 1.0F);

      }
      serverRow.update(MLProtos.RowType.T_FLOAT_DENSE, byteBuf, 100);



      // sparse
      byteBuf = Mockito.mock(ByteBuf.class);
      ByteBuf valueBuf = Mockito.mock(ByteBuf.class);
      when(byteBuf.slice(4 * 100, 4 * 100)).thenReturn(valueBuf);
      loop = null;
      valueLoop = null;


      for (int updateIdx = 0; updateIdx < 100; updateIdx++) {
        if (loop == null) {
          loop = when(byteBuf.readFloat());
        }
        loop = loop.thenReturn(updateIdx * 1.0F);

        if (valueLoop == null) {
          valueLoop = when(valueBuf.readFloat());
        }
        valueLoop = valueLoop.thenReturn(updateIdx * 1.0F);

      }
      serverRow.update(MLProtos.RowType.T_FLOAT_SPARSE, byteBuf, 100);

      when(serverPartition.getRow(i)).thenReturn(serverRow);
    }
    partitionAggrResult = sumAggrFunc.aggr(new PartitionAggrParam(METRIC_ID, PARTITION_KEY_1));
    Assert.assertTrue(partitionAggrResult != null);
    Assert.assertTrue(
        ((SumAggrFunc.SumAggrResult) sumAggrFunc.merge(Arrays.asList(partitionAggrResult)))
            .getResult() == 990000D);

  }



  @Test
  public void testSplit() throws Exception {
    List<PartitionAggrParam> partitionAggrParams = sumAggrFunc.getParam().split();
    Assert.assertTrue(partitionAggrParams.size() == 2);
  }



} 
