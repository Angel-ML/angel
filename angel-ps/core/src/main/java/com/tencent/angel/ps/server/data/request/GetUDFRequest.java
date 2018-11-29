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


package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ps.server.data.TransportMethod;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Get udf rpc request.
 */
public class GetUDFRequest extends PartitionRequest {
  private static final Log LOG = LogFactory.getLog(GetUDFRequest.class);

  /**
   * the udf class name
   */
  private String getFuncClass;

  /**
   * partition parameter of the udf
   */
  private PartitionGetParam partParam;

  /**
   * Create a new GetUDFRequest.
   *
   * @param userRequestId user request id
   * @param partKey       matrix partition key
   * @param getFuncClass  udf class name
   * @param partParam     partition parameter of the udf
   */
  public GetUDFRequest(int userRequestId, PartitionKey partKey, String getFuncClass,
    PartitionGetParam partParam) {
    super(userRequestId, 0, partKey);
    this.getFuncClass = getFuncClass;
    this.partParam = partParam;
  }

  /**
   * Create a new GetUDFRequest.
   */
  public GetUDFRequest() {
    this(-1, null, null, null);
  }

  @Override public TransportMethod getType() {
    return TransportMethod.GET_PSF;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (getFuncClass != null) {
      byte[] data = getFuncClass.getBytes();
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

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if (buf.isReadable()) {
      int size = buf.readInt();
      byte[] data = new byte[size];
      buf.readBytes(data);
      getFuncClass = new String(data);
    }

    if (buf.isReadable()) {
      int size = buf.readInt();
      byte[] data = new byte[size];
      buf.readBytes(data);
      String partParamClassName = new String(data);
      try {
        partParam = (PartitionGetParam) Class.forName(partParamClassName).newInstance();
        partParam.deserialize(buf);
      } catch (Exception e) {
        LOG.fatal("deserialize PartitionAggrParam falied, ", e);
      }
    }
  }

  @Override public int bufferLen() {
    int size = super.bufferLen();
    if (getFuncClass != null) {
      size += 4;
      size += getFuncClass.toCharArray().length;
    }

    if (partParam != null) {
      size += 4;
      size += partParam.bufferLen();
    }

    return size;
  }

  @Override public int getEstimizeDataSize() {
    return 1;
  }

  /**
   * Get udf class name.
   *
   * @return String udf class name
   */
  public String getGetFuncClass() {
    return getFuncClass;
  }

  /**
   * Get the partition parameter of the udf.
   *
   * @return PartitionGetParam the partition parameter of the udf
   */
  public PartitionGetParam getPartParam() {
    return partParam;
  }


  @Override public String toString() {
    return "GetUDFRequest{" + "getFuncClass='" + getFuncClass + '\'' + ", partParam=" + partParam
      + "} " + super.toString();
  }
}