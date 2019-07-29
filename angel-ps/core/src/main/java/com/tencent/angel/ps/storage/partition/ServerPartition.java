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


package com.tencent.angel.ps.storage.partition;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.matrix.PartitionState;
import com.tencent.angel.ps.storage.partition.storage.IServerPartitionStorage;
import com.tencent.angel.ps.storage.partition.storage.ServerPartitionStorageFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import java.io.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The Server partition represents a partition of matrix.
 */
public abstract class ServerPartition implements IServerPartition {

  private final static Log LOG = LogFactory.getLog(
      ServerPartition.class);

  /**
   * Partition key
   */
  protected PartitionKey partKey;

  /**
   * Server Matrix row type
   */
  protected RowType rowType;

  /**
   * Estimate sparsity for sparse model type
   */
  protected final double estSparsity;

  /**
   * Partition clock, it is the minimal clock value in clock vector
   */
  private volatile int clock;

  /**
   * Partition state
   */
  private volatile PartitionState state;

  /**
   * Partition storage
   */
  private IServerPartitionStorage storage;

  /**
   * Create a new Server partition,include load rows.
   *
   * @param partKey the partition meta
   * @param rowType row type
   * @param estSparsity valid element number / index range
   * @param storage partition storage
   */
  public ServerPartition(PartitionKey partKey, RowType rowType, double estSparsity,
      IServerPartitionStorage storage) {
    this.state = PartitionState.INITIALIZING;
    this.partKey = partKey;
    this.rowType = rowType;
    this.clock = 0;
    this.estSparsity = estSparsity;
    this.storage = storage;
  }


  /**
   * Create a new Server partition.
   */
  public ServerPartition() {
    this(null, RowType.T_DOUBLE_DENSE, 1.0, null);
  }


  @Override
  public void update(UpdateFunc func, PartitionUpdateParam partParam) {
    func.partitionUpdate(partParam);
  }

  @Override
  public void update(ByteBuf buf, UpdateOp op) {
    getStorage().update(buf, op);
  }


  @Override
  public void serialize(ByteBuf buf) {
    // Serialize the head
    partKey.serialize(buf);
    buf.writeInt(rowType.getNumber());

    // Serialize the storage
    byte[] data = storage.getClass().getName().getBytes();
    buf.writeInt(data.length);
    buf.writeBytes(data);
    storage.serialize(buf);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    // Deserialize the head
    partKey = new PartitionKey();
    partKey.deserialize(buf);
    rowType = RowType.valueOf(buf.readInt());

    // Deseralize the storage
    int size = buf.readInt();
    byte[] data = new byte[size];
    buf.readBytes(data);
    String storageClassName = new String(data);

    storage = ServerPartitionStorageFactory.getStorage(storageClassName);
    storage.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    return partKey.bufferLen() + 4 + 4 + storage.getClass().getName().getBytes().length + storage
        .bufferLen();
  }

  /**
   * Gets related partition key.
   *
   * @return the partition key
   */
  public PartitionKey getPartitionKey() {
    return partKey;
  }

  /**
   * Get partition storage
   *
   * @return partition storage
   */
  public IServerPartitionStorage getStorage() {
    return storage;
  }

  /**
   * Set partition storage
   *
   * @param storage partition storage
   */
  public void setStorage(IServerPartitionStorage storage) {
    this.storage = storage;
  }

  /**
   * Get partition clock
   *
   * @return partition clock
   */
  public int getClock() {
    return clock;
  }

  /**
   * Set partition clock
   *
   * @param clock partition clock
   */
  public void setClock(int clock) {
    this.clock = clock;
  }

  /**
   * Set partition state
   *
   * @param state partition state
   */
  public void setState(PartitionState state) {
    this.state = state;
  }

  /**
   * Get partition state
   *
   * @return partition tate
   */
  public PartitionState getState() {
    return state;
  }

  /**
   * Get row type
   *
   * @return row type
   */
  public RowType getRowType() {
    return rowType;
  }

  /**
   * Get valid element number / index range
   *
   * @return valid element number / index range
   */
  public double getEstSparsity() {
    return estSparsity;
  }

  @Override
  public long getElemNum() {
    return getStorage().getElemNum();
  }
}
