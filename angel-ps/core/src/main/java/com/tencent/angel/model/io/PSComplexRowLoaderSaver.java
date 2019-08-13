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

package com.tencent.angel.model.io;

import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.model.output.format.ComplexRowFormat;
import com.tencent.angel.model.output.format.IndexAndElement;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowPartitionMeta;
import com.tencent.angel.ps.storage.matrix.PartitionState;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.storage.IntArrayElementStorage;
import com.tencent.angel.ps.storage.vector.storage.IntElementMapStorage;
import com.tencent.angel.ps.storage.vector.storage.IntElementStorage;
import com.tencent.angel.ps.storage.vector.storage.LongElementStorage;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
public class PSComplexRowLoaderSaver extends PSMatrixLoaderSaverImpl {

  private final ComplexRowFormat format;
  public PSComplexRowLoaderSaver(ComplexRowFormat format, Configuration conf) {
    super(conf);
    this.format = format;
  }

  public PSComplexRowLoaderSaver(ComplexRowFormat format, Configuration conf, int partNumPerFile) {
    super(conf, partNumPerFile);
    this.format = format;
  }


  @Override
  public void load(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    RowBasedPartition rowPart = (RowBasedPartition) part;
    try {
      Map<Integer, RowPartitionMeta> rowMetas = partMeta.getRowMetas();
      for (RowPartitionMeta rowMeta : rowMetas.values()) {
        ServerRow row = rowPart.getRow(rowMeta.getRowId());
        load(row, partMeta, loadContext, input);
      }
    } finally {
      part.setState(PartitionState.READ_AND_WRITE);
    }
  }

  private void load(ServerRow row, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    try {
      row.startWrite();
      if(row instanceof ServerIntAnyRow) {
        load((ServerIntAnyRow) row, partMeta, loadContext, input);
      } else if(row instanceof ServerLongAnyRow) {
        load((ServerLongAnyRow) row, partMeta, loadContext, input);
      } else {
        throw new UnsupportedOperationException("Unsupport row type" + row.getClass());
      }
    } finally {
      row.endWrite();
    }
  }

  private void load(ServerIntAnyRow row, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    RowPartitionMeta rowMeta = partMeta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();

    for(int i = 0; i < elemNum; i++) {
      IndexAndElement indexAndElement = format.load(input);
      row.set((int)indexAndElement.index, indexAndElement.element);
    }
  }

  private void load(ServerLongAnyRow row, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    RowPartitionMeta rowMeta = partMeta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();

    for(int i = 0; i < elemNum; i++) {
      IndexAndElement indexAndElement = format.load(input);
      row.set(indexAndElement.index, indexAndElement.element);
    }
  }

  @Override
  public void save(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    List<Integer> rowIds = saveContext.getRowIndexes();
    RowBasedPartition rowPart = (RowBasedPartition) part;

    if (rowIds == null || rowIds.isEmpty()) {
      Iterator<Entry<Integer, ServerRow>> iter = rowPart.getRowsStorage().iterator();
      rowIds = new ArrayList<>();
      while (iter.hasNext()) {
        rowIds.add(iter.next().getKey());
      }
    } else {
      rowIds = filter(rowPart, rowIds);
    }

    FSDataOutputStream dataOutputStream =
        new FSDataOutputStream(output, null, partMeta != null ? partMeta.getOffset() : 0);

    partMeta.setSaveRowNum(rowIds.size());
    for (int rowId : rowIds) {
      ServerRow row = rowPart.getRow(rowId);
      RowPartitionMeta rowMeta = new RowPartitionMeta(rowId, 0, 0);
      if (row != null) {
        rowMeta.setElementNum(row.size());
        rowMeta.setOffset(dataOutputStream.getPos());
        save(rowPart.getRow(rowId), saveContext, partMeta, output);
      } else {
        rowMeta.setElementNum(0);
        rowMeta.setOffset(dataOutputStream.getPos());
      }
      partMeta.setRowMeta(rowMeta);
    }
  }

  private void save(ServerRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta partMeta, DataOutputStream output)
      throws IOException {
    if(saveContext.cloneFirst()) {
      row = (ServerRow) row.adaptiveClone();
    }

    if(row instanceof ServerIntAnyRow) {
      save((ServerIntAnyRow) row, saveContext, partMeta, output);
    } else if(row instanceof ServerLongAnyRow) {
      save((ServerLongAnyRow) row, saveContext, partMeta, output);
    } else {
      throw new UnsupportedOperationException("Unsupport row type" + row.getClass());
    }
  }

  private void save(ServerLongAnyRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta partMeta, DataOutputStream output)
      throws IOException {
    LongElementStorage storage = row.getStorage();
    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = storage
        .iterator();
    long startPos = partMeta.getStartCol();
    while(iter.hasNext()) {
      Long2ObjectMap.Entry<IElement> entry = iter.next();
      format.save(entry.getLongKey() + startPos, entry.getValue(), output);
    }
  }

  private void save(ServerIntAnyRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta partMeta, DataOutputStream output)
      throws IOException {
    IntElementStorage storage = row.getStorage();
    long startPos = partMeta.getStartCol();
    if(storage instanceof IntArrayElementStorage) {
      IElement [] data = ((IntArrayElementStorage) storage).getData();
      for(int i = 0; i < data.length; i++) {
        format.save(i +  startPos, data[i], output);
      }
    } else if(storage instanceof IntElementMapStorage) {
      Int2ObjectOpenHashMap<IElement> data = ((IntElementMapStorage) storage)
          .getData();
      ObjectIterator<Int2ObjectMap.Entry<IElement>> iter = data
          .int2ObjectEntrySet().fastIterator();
      while(iter.hasNext()) {
        Int2ObjectMap.Entry<IElement> entry = iter.next();
        format.save(entry.getIntKey() + startPos, entry.getValue(), output);
      }
    }
  }

  protected List<Integer> filter(ServerPartition part, List<Integer> rowIds) {
    List<Integer> ret = new ArrayList<>();
    for (int rowId : rowIds) {
      if (((RowBasedPartition) part).hasRow(rowId)) {
        ret.add(rowId);
      }
    }

    return ret;
  }
}
