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

package com.tencent.angel.worker;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.vector.*;
import com.tencent.angel.psagent.matrix.oplog.cache.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.*;

import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class VectorSplitTest {

  private final static Log LOG = LogFactory.getLog(VectorSplitTest.class);
  
  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @BeforeClass
  public static void beforeClass() {

  }

  @Test
  public void sparseSortedVectorSplit() {
    int[] offsets = {0, 2, 4, 6, 8};
    double[] values = {0.0, 2.0, 4.0, 6.0, 8.0};
    SparseIntDoubleSortedVector vector = new SparseIntDoubleSortedVector(10, offsets, values);
    vector.setRowId(0);

    PartitionKey key1 = new PartitionKey(0, 0, 0, 0, 1, 5);
    PartitionKey key2 = new PartitionKey(1, 0, 0, 5, 1, 10);

    List<PartitionKey> keys = new ArrayList<>();
    keys.add(key1);
    keys.add(key2);

    HashMap<PartitionKey, RowUpdateSplit> splits = RowUpdateSplitUtils.split(vector, keys);
    Assert.assertEquals(keys.size(), splits.size());

    int[] offset1 = {0, 2, 4};
    double[] values1 = {0.0, 2.0, 4.0};
    SparseDoubleRowUpdateSplit split1 = (SparseDoubleRowUpdateSplit) splits.get(key1);
    Assert.assertNotNull(split1);
    Assert.assertEquals(offset1.length, split1.size());
    Assert.assertEquals(0, split1.getStart());
    Assert.assertEquals(3, split1.getEnd());


    Assert.assertArrayEquals(offset1,
        Arrays.copyOfRange(split1.getOffsets(), (int)split1.getStart(), (int)split1.getEnd()));
    for (int i = 0; i < split1.size(); i++) {
      Assert.assertEquals(values1[i], split1.getValues()[(int)split1.getStart() + i], 0.0);
    }

    int[] offset2 = {6, 8};
    double[] values2 = {6.0, 8.0};
    SparseDoubleRowUpdateSplit split2 = (SparseDoubleRowUpdateSplit) splits.get(key2);
    Assert.assertNotNull(split2);
    Assert.assertEquals(offset2.length, split2.size());
    Assert.assertEquals(3, split2.getStart());
    Assert.assertEquals(5, split2.getEnd());


    Assert.assertArrayEquals(offset2,
        Arrays.copyOfRange(split2.getOffsets(), (int)split2.getStart(), (int)split2.getEnd()));
    for (int i = 0; i < split2.size(); i++) {
      Assert.assertEquals(values2[i], split2.getValues()[(int)split2.getStart() + i], 0.0);
    }

    LOG.info("Pass sparseSortedVectorSplit Test");
  }

  @Test
  public void denseVectorSplit() {
    double[] values = {0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0};
    DenseIntDoubleVector vector = new DenseIntDoubleVector(10, values);
    vector.setRowId(0);

    PartitionKey key1 = new PartitionKey(0, 0, 0, 0, 1, 5);
    PartitionKey key2 = new PartitionKey(1, 0, 0, 5, 1, 10);

    List<PartitionKey> keys = new ArrayList<>();
    keys.add(key1);
    keys.add(key2);

    HashMap<PartitionKey, RowUpdateSplit> splits = RowUpdateSplitUtils.split(vector, keys);
    Assert.assertEquals(keys.size(), splits.size());

    double[] values1 = {0.0, 1.0, 2.0, 3.0, 4.0};
    DenseDoubleRowUpdateSplit split1 = (DenseDoubleRowUpdateSplit) splits.get(key1);
    Assert.assertNotNull(split1);
    Assert.assertEquals(values1.length, split1.size());
    Assert.assertEquals(0, split1.getStart());
    Assert.assertEquals(5, split1.getEnd());

    for (int i = 0; i < split1.size(); i++) {
      Assert.assertEquals(values1[i], split1.getValues()[(int)split1.getStart() + i], 0.0);
    }

    double[] values2 = {5.0, 6.0, 7.0, 8.0, 9.0};
    DenseDoubleRowUpdateSplit split2 = (DenseDoubleRowUpdateSplit) splits.get(key2);
    Assert.assertNotNull(split2);
    Assert.assertEquals(values2.length, split2.size());
    Assert.assertEquals(5, split2.getStart());
    Assert.assertEquals(10, split2.getEnd());


    for (int i = 0; i < split2.size(); i++) {
      Assert.assertEquals(values2[i], split2.getValues()[(int)split2.getStart() + i], 0.0);
    }

    LOG.info("Pass denseVectorSplit Test");
  }

  @Test
  public void denseIntVectorSplit() {
    int[] values = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    DenseIntVector vector = new DenseIntVector(10, values);
    vector.setRowId(0);

    PartitionKey key1 = new PartitionKey(0, 0, 0, 0, 1, 5);
    PartitionKey key2 = new PartitionKey(1, 0, 0, 5, 1, 10);

    List<PartitionKey> keys = new ArrayList<>();
    keys.add(key1);
    keys.add(key2);

    HashMap<PartitionKey, RowUpdateSplit> splits = RowUpdateSplitUtils.split(vector, keys);
    Assert.assertEquals(keys.size(), splits.size());

    int[] values1 = {0, 1, 2, 3, 4};
    DenseIntRowUpdateSplit split1 = (DenseIntRowUpdateSplit) splits.get(key1);
    Assert.assertNotNull(split1);
    Assert.assertEquals(values1.length, split1.size());
    Assert.assertEquals(0, split1.getStart());
    Assert.assertEquals(5, split1.getEnd());

    for (int i = 0; i < split1.size(); i++) {
      Assert.assertEquals(values1[i], split1.getValues()[(int)split1.getStart() + i]);
    }

    int[] values2 = {5, 6, 7, 8, 9};
    DenseIntRowUpdateSplit split2 = (DenseIntRowUpdateSplit) splits.get(key2);
    Assert.assertNotNull(split2);
    Assert.assertEquals(values2.length, split2.size());
    Assert.assertEquals(5, split2.getStart());
    Assert.assertEquals(10, split2.getEnd());


    for (int i = 0; i < split2.size(); i++) {
      Assert.assertEquals(values2[i], split2.getValues()[(int)split2.getStart() + i]);
    }

    LOG.info("Pass denseIntVectorSplit Test");
  }

  @Test
  public void sparseHashMapVectorSplit() {
    int[] offsets = {0, 2, 4, 6, 8};
    double[] values = {0.0, 2.0, 4.0, 6.0, 8.0};
    SparseIntDoubleVector vector = new SparseIntDoubleVector(10, offsets, values);
    vector.setRowId(0);

    PartitionKey key1 = new PartitionKey(0, 0, 0, 0, 1, 5);
    PartitionKey key2 = new PartitionKey(1, 0, 0, 5, 1, 10);

    List<PartitionKey> keys = new ArrayList<>();
    keys.add(key1);
    keys.add(key2);

    HashMap<PartitionKey, RowUpdateSplit> splits = RowUpdateSplitUtils.split(vector, keys);
    Assert.assertEquals(keys.size(), splits.size());

    int[] offset1 = {0, 2, 4};
    double[] values1 = {0.0, 2.0, 4.0};
    SparseDoubleRowUpdateSplit split1 = (SparseDoubleRowUpdateSplit)splits.get(key1);
    Assert.assertNotNull(split1);
    Assert.assertEquals(2, splits.get(key2).size());
    Assert.assertEquals(offset1.length, split1.size());
    Assert.assertEquals(0, split1.getStart());
    Assert.assertEquals(3, split1.getEnd());


    Assert.assertArrayEquals(offset1,
        Arrays.copyOfRange(split1.getOffsets(), (int)split1.getStart(), (int)split1.getEnd()));
    for (int i = 0; i < split1.size(); i++) {
      Assert.assertEquals(values1[i], split1.getValues()[(int)split1.getStart() + i], 0.0);
    }

    int[] offset2 = {6, 8};
    double[] values2 = {6.0, 8.0};
    SparseDoubleRowUpdateSplit split2 = (SparseDoubleRowUpdateSplit) splits.get(key2);
    Assert.assertNotNull(split2);
    Assert.assertEquals(offset2.length, split2.size());
    Assert.assertEquals(3, split2.getStart());
    Assert.assertEquals(5, split2.getEnd());


    Assert.assertArrayEquals(offset2,
        Arrays.copyOfRange(split2.getOffsets(), (int)split2.getStart(), (int)split2.getEnd()));
    for (int i = 0; i < split2.size(); i++) {
      Assert.assertEquals(values2[i], split2.getValues()[(int)split2.getStart() + i], 0.0);
    }

    LOG.info("Pass sparseHashMapVector split Test");
  }

  @Test
  public void sparseIntVectorSplit() {
    int[] offsets = {0, 2, 4, 6, 8};
    int[] values = {0, 2, 4, 6, 8};
    SparseIntVector vector = new SparseIntVector(10, offsets, values);
    vector.setRowId(0);

    PartitionKey key1 = new PartitionKey(0, 0, 0, 0, 1, 5);
    PartitionKey key2 = new PartitionKey(1, 0, 0, 5, 1, 10);

    List<PartitionKey> keys = new ArrayList<>();
    keys.add(key1);
    keys.add(key2);

    HashMap<PartitionKey, RowUpdateSplit> splits = RowUpdateSplitUtils.split(vector, keys);
    Assert.assertEquals(keys.size(), splits.size());

    int[] offset1 = {0, 2, 4};
    int[] values1 = {0, 2, 4};
    SparseIntRowUpdateSplit split1 = (SparseIntRowUpdateSplit)splits.get(key1);
    Assert.assertNotNull(split1);
    Assert.assertEquals(offset1.length, split1.size());
    Assert.assertEquals(0, split1.getStart());
    Assert.assertEquals(3, split1.getEnd());


    Assert.assertArrayEquals(offset1,
        Arrays.copyOfRange(split1.getOffsets(), (int)split1.getStart(), (int)split1.getEnd()));
    for (int i = 0; i < split1.size(); i++) {
      Assert.assertEquals(values1[i], split1.getValues()[(int)split1.getStart() + i], 0.0);
    }

    int[] offset2 = {6, 8};
    int[] values2 = {6, 8};
    SparseIntRowUpdateSplit split2 = (SparseIntRowUpdateSplit)splits.get(key2);
    Assert.assertNotNull(split2);
    Assert.assertEquals(offset2.length, split2.size());
    Assert.assertEquals(3, split2.getStart());
    Assert.assertEquals(5, split2.getEnd());


    Assert.assertArrayEquals(offset2,
        Arrays.copyOfRange(split2.getOffsets(), (int)split2.getStart(), (int)split2.getEnd()));
    for (int i = 0; i < split2.size(); i++) {
      Assert.assertEquals(values2[i], split2.getValues()[(int)split2.getStart() + i], 0.0);
    }

    LOG.info("Pass sparseIntVector split Test");
  }

  @Test
  public void mockitoTest() {
    List list = mock(List.class);

    // using mock object
    list.add("one");
    list.clear();

    // verification
    verify(list).add("one");
    verify(list).clear();

    // Stubbing
    when(list.get(0)).thenReturn("first");
    when(list.get(1)).thenThrow(new RuntimeException("list.get(1)"));

    Assert.assertEquals("first", list.get(0));
    Assert.assertEquals(isNull(), list.get(999));

    verify(list).get(999);

    when(list.get(anyInt())).thenReturn("element");

    Assert.assertEquals("element", list.get(100));
    verify(list).get(100);

    list.add("once");

    list.add("twice");
    list.add("twice");

    list.add("three times");
    list.add("three times");
    list.add("three times");

    verify(list).add("once");
    verify(list, times(2)).add("twice");
    verify(list, times(3)).add("three times");
    verify(list, never()).add("never happened");

    List firstMock = mock(List.class);
    List secondMock = mock(List.class);

    firstMock.add("was called first");
    secondMock.add("was called second");

    InOrder inOrder = inOrder(firstMock, secondMock);

    inOrder.verify(firstMock).add("was called first");
    inOrder.verify(secondMock).add("was called second");

    List mockOne = mock(List.class);
    List mockTwo = mock(List.class);
    mockOne.add("one");

    verify(mockOne).add("one");
    verifyNoMoreInteractions(mockOne);

    verifyZeroInteractions(mockTwo);

    when(mockOne.get(0)).thenReturn("Value1").thenReturn("Value2");

    Assert.assertEquals("Value1", mockOne.get(0));
    Assert.assertEquals("Value2", mockOne.get(0));
    Assert.assertEquals("Value2", mockOne.get(0));


    List mockAnswer = mock(List.class);
    when(mockAnswer.get(anyInt())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        int index = (int) args[0];
        return String.valueOf(index);
      }
    });

    Assert.assertEquals("5", mockAnswer.get(5));

    List realList = new LinkedList();
    List spy = spy(realList);

    when(spy.size()).thenReturn(100);
    Assert.assertEquals(100, spy.size());

    spy.add("one");
    spy.add("two");
    verify(spy).add("one");
    verify(spy).add("two");

    LOG.info("Pass mockito Test");

  }

}
