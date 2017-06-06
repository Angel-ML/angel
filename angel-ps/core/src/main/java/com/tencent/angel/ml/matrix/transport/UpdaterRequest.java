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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.updater.base.PartitionUpdaterParam;
import com.tencent.angel.ps.ParameterServerId;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Update udf rpc request.
 */
public class UpdaterRequest extends PartitionRequest {
  private static final Log LOG = LogFactory.getLog(UpdaterRequest.class);
  /** update udf function class name */
  private String updaterFuncClass;

  /** the partition parameter of the update udf */
  private PartitionUpdaterParam partParam;

  /**
   * Create a new UpdaterRequest.
   *
   * @param serverId parameter server id
   * @param partKey matrix partition key
   * @param updaterFuncClass update udf function class name
   * @param partParam the partition parameter of the update udf
   */
  public UpdaterRequest(ParameterServerId serverId, PartitionKey partKey, String updaterFuncClass,
      PartitionUpdaterParam partParam) {
    super(serverId, 0, partKey);
    this.updaterFuncClass = updaterFuncClass;
    this.partParam = partParam;
  }

  /**
   * Create a new UpdaterRequest.
   */
  public UpdaterRequest() {
    this(null, null, null, null);
  }


  @Override
  public int getEstimizeDataSize() {
    return 4;
  }

  @Override
  public TransportMethod getType() {
    return TransportMethod.UPDATER;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (updaterFuncClass != null) {
      byte[] data = updaterFuncClass.getBytes();
      buf.writeInt(data.length);
      buf.writeBytes(data);
    }

    if (partParam != null) {
      String partParamClassName = partParam.getClass().getName();
      byte[] data = partParamClassName.getBytes();
      buf.writeInt(data.length);
      buf.writeBytes(data);
      partParam.serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if (buf.isReadable()) {
      int size = buf.readInt();
      byte[] data = new byte[size];
      buf.readBytes(data);
      updaterFuncClass = new String(data);
    }

    if (buf.isReadable()) {
      int size = buf.readInt();
      byte[] data = new byte[size];
      buf.readBytes(data);
      String partParamClassName = new String(data);
      try {
        partParam = (PartitionUpdaterParam) Class.forName(partParamClassName).newInstance();
        partParam.deserialize(buf);
      } catch (Exception e) {
        LOG.fatal("deserialize PartitionAggrParam falied, ", e);
      }
    }
  }

  @Override
  public int bufferLen() {
    int size = super.bufferLen();
    if (updaterFuncClass != null) {
      size += 4;
      size += updaterFuncClass.toCharArray().length;
    }

    if (partParam != null) {
      size += 4;
      size += partParam.bufferLen();
    }

    return size;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((partParam == null) ? 0 : partParam.hashCode());
    result = prime * result + ((updaterFuncClass == null) ? 0 : updaterFuncClass.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj;
  }

  /**
   * Get update udf function class name.
   * 
   * @return String update udf function class name
   */
  public String getUpdaterFuncClass() {
    return updaterFuncClass;
  }

  /**
   * Get the partition parameter of the update udf.
   * 
   * @return PartitionUpdaterParam the partition parameter of the update udf
   */
  public PartitionUpdaterParam getPartParam() {
    return partParam;
  }

  /**
   * Set update udf function class name.
   * 
   * @param updaterFuncClass update udf function class name
   */
  public void setUpdaterFuncClass(String updaterFuncClass) {
    this.updaterFuncClass = updaterFuncClass;
  }

  /**
   * Set the partition parameter of the update udf.
   * 
   * @param PartitionUpdaterParam the partition parameter of the update udf
   */
  public void setPartParam(PartitionUpdaterParam partParam) {
    this.partParam = partParam;
  }

  @Override
  public String toString() {
    return "UpdaterRequest [updaterFuncClass=" + updaterFuncClass + ", partParam=" + partParam
        + ", toString()=" + super.toString() + "]";
  }
}
