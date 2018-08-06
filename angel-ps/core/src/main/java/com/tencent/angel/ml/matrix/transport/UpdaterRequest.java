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

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
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
  private PartitionUpdateParam partParam;

  /**
   * Create a new UpdaterRequest.
   *
   * @param partKey matrix partition key
   * @param updaterFuncClass update udf function class name
   * @param partParam the partition parameter of the update udf
   */
  public UpdaterRequest(PartitionKey partKey, String updaterFuncClass,
      PartitionUpdateParam partParam) {
    super(0, partKey);
    this.updaterFuncClass = updaterFuncClass;
    this.partParam = partParam;
  }

  /**
   * Create a new UpdaterRequest.
   */
  public UpdaterRequest() {
    this(null, null, null);
  }


  @Override
  public int getEstimizeDataSize() {
    return bufferLen();
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
        partParam = (PartitionUpdateParam) Class.forName(partParamClassName).newInstance();
        partParam.deserialize(buf);
      } catch (Exception e) {
        LOG.error("deserialize PartitionAggrParam falied, ", e);
        throw new RuntimeException("deserialize update psf parameter failed:" + e.getMessage());
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
   * @return PartitionUpdateParam the partition parameter of the update udf
   */
  public PartitionUpdateParam getPartParam() {
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
   * @param partParam the partition parameter of the update udf
   */
  public void setPartParam(PartitionUpdateParam partParam) {
    this.partParam = partParam;
  }

  @Override
  public String toString() {
    return "UpdaterRequest [updaterFuncClass=" + updaterFuncClass + ", partParam=" + partParam
        + ", toString()=" + super.toString() + "]";
  }
}
