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

import com.tencent.angel.ps.impl.matrix.*;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.psagent.PSAgentContext;

/**
 * The Scalar updater.
 */
public class ScalarUpdater extends DefaultUpdaterFunc{
  private static final Log LOG = LogFactory.getLog(ScalarUpdater.class);

  /**
   * Instantiates a new Scalar updater.
   *
   * @param param the param
   */
  public ScalarUpdater(ScalarUpdaterParam param) {
    super(param);
  }

  /**
   * Creates a new Scalar updater.
   */
  public ScalarUpdater() {
    super(null);
  }


  /**
   * The parameter of  Scalar updater.
   */
  public static class ScalarUpdaterParam extends UpdaterParam{
    private final double scalarFactor;

    /**
     * Creates a new parameter.
     *
     * @param matrixId     the matrix id
     * @param updateClock  the update clock
     * @param scalarFactor the scalar factor
     */
    public ScalarUpdaterParam(int matrixId, boolean updateClock, double scalarFactor) {
      super(matrixId, updateClock);
      this.scalarFactor = scalarFactor;
    }

    @Override
    public List<PartitionUpdaterParam> split() {
      List<PartitionKey> partList = PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);
      int size = partList.size();
      List<PartitionUpdaterParam> partParamList = new ArrayList<PartitionUpdaterParam>(size);
      for(int i = 0; i < size; i++){
        partParamList.add(new ScalarPartitionUpdaterParam(matrixId, partList.get(i), updateClock, scalarFactor));
      }
      
      return partParamList;
    }

    /**
     * Gets scalar factor.
     *
     * @return the scalar factor
     */
    public double getScalarFactor() {
      return scalarFactor;
    } 
  }

  /**
   * The parameter of Scalar partition updater.
   */
  public static class ScalarPartitionUpdaterParam extends PartitionUpdaterParam{
    private double scalarFactor;

    /**
     * Creates a new  partition parameter.
     *
     * @param matrixId     the matrix id
     * @param partKey      the part key
     * @param updateClock  the update clock
     * @param scalarFactor the scalar factor
     */
    public ScalarPartitionUpdaterParam(int matrixId, PartitionKey partKey, boolean updateClock, double scalarFactor){
      super(matrixId, partKey, updateClock);
      this.scalarFactor = scalarFactor;
    }

    /**
     * Creates a new  partition parameter by default.
     */
    public ScalarPartitionUpdaterParam(){
      super();
      scalarFactor = 1.0;
    }
    
    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeDouble(scalarFactor);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      scalarFactor = buf.readDouble();
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 8;
    }

    /**
     * Gets scalar factor.
     *
     * @return the scalar factor
     */
    public double getScalarFactor() {
      return scalarFactor;
    }

    /**
     * Sets scalar factor.
     *
     * @param scalarFactor the scalar factor
     */
    public void setScalarFactor(double scalarFactor) {
      this.scalarFactor = scalarFactor;
    }

    @Override
    public String toString() {
      return "ScalarPartitionUpdaterParam [scalarFactor=" + scalarFactor + ", toString()="
          + super.toString() + "]";
    }
  }

  @Override
  public void partitionUpdate(PartitionUpdaterParam partParam) {
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      int startRow = part.getPartitionKey().getStartRow();
      int endRow = part.getPartitionKey().getEndRow();
      double scalarFactor = ((ScalarPartitionUpdaterParam)partParam).scalarFactor;
      for (int i = startRow; i < endRow; i++) {
        ServerRow row = part.getRow(i);
        if(row == null){
          continue;
        }
        update(row, scalarFactor);
      }
    }

    return;
  }

  private void update(ServerRow row, double scalarFactor) {
    switch (row.getRowType()) {
      case T_DOUBLE_SPARSE:
        update((ServerSparseDoubleRow) row, scalarFactor);
        break;

      case T_DOUBLE_DENSE:
        update((ServerDenseDoubleRow) row, scalarFactor);
        break;

      case T_FLOAT_SPARSE:
        update((ServerSparseFloatRow) row, scalarFactor);
        break;

      case T_FLOAT_DENSE:
        update((ServerDenseFloatRow) row, scalarFactor);
        break;

      case T_INT_SPARSE:
        update((ServerSparseIntRow) row, scalarFactor);
        break;

      case T_INT_DENSE:
        update((ServerDenseIntRow) row, scalarFactor);
        break;

      default:
        break;
    }
  }
  
  private void update(ServerSparseDoubleRow row, double scalarFactor) {
    try {
      row.getLock().writeLock().lock();
      ObjectIterator<it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry> iter =
          row.getData().int2DoubleEntrySet().fastIterator();
      it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        entry.setValue(entry.getDoubleValue() * scalarFactor);
      }

    } finally {
      row.getLock().writeLock().unlock();
    }
  }

  private void update(ServerDenseDoubleRow row, double scalarFactor) {
    try {
      row.getLock().writeLock().lock();
      DoubleBuffer data = row.getData();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        data.put(i, data.get(i) * scalarFactor);
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }


  private void update(ServerSparseFloatRow row, double scalarFactor) {
    try {
      row.getLock().writeLock().lock();
      ObjectIterator<it.unimi.dsi.fastutil.ints.Int2FloatMap.Entry> iter =
          row.getData().int2FloatEntrySet().fastIterator();
      it.unimi.dsi.fastutil.ints.Int2FloatMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        entry.setValue((float) (entry.getFloatValue() * scalarFactor));
      }

    } finally {
      row.getLock().writeLock().unlock();
    }
  }

  private void update(ServerDenseFloatRow row, double scalarFactor) {
    try {
      row.getLock().writeLock().lock();
      FloatBuffer data = row.getData();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        data.put(i, (float) (data.get(i) * scalarFactor));
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

  private void update(ServerSparseIntRow row, double scalarFactor) {
    try {
      row.getLock().writeLock().lock();
      ObjectIterator<it.unimi.dsi.fastutil.ints.Int2IntMap.Entry> iter =
          row.getData().int2IntEntrySet().fastIterator();
      
      it.unimi.dsi.fastutil.ints.Int2IntMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        entry.setValue((int) (entry.getIntValue() * scalarFactor));
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }
  
  private void update(ServerDenseIntRow row, double scalarFactor) {
    try {
      row.getLock().writeLock().lock();
      IntBuffer data = row.getData();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        data.put(i, (int) (data.get(i) * scalarFactor));
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }
}
