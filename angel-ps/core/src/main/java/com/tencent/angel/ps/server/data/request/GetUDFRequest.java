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

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Get udf rpc request.
 */
public class GetUDFRequest extends RequestData {
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
   * @param getFuncClass  udf class name
   * @param partParam     partition parameter of the udf
   */
  public GetUDFRequest(String getFuncClass, PartitionGetParam partParam) {
    this.getFuncClass = getFuncClass;
    this.partParam = partParam;
  }

  /**
   * Create a new GetUDFRequest.
   */
  public GetUDFRequest() {
    this(null, null);
  }

  @Override public void serialize(ByteBuf buf) {
    ByteBufSerdeUtils.serializeUTF8(buf, getFuncClass);
    ByteBufSerdeUtils.serializeObject(buf, partParam);
  }

  @Override public void deserialize(ByteBuf buf) {
    int readerIndex = buf.readerIndex();
    getFuncClass = ByteBufSerdeUtils.deserializeUTF8(buf);
    partParam = (PartitionGetParam) ByteBufSerdeUtils.deserializeObject(buf);
    requestSize = buf.readerIndex() - readerIndex;
  }

  @Override public int bufferLen() {
    int len = ByteBufSerdeUtils.serializedUTF8Len(getFuncClass);
    len += ByteBufSerdeUtils.serializedObjectLen(partParam);
    return len;
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

  @Override
  public String toString() {
    return "GetUDFRequest{" +
        "getFuncClass='" + getFuncClass + '\'' +
        ", partParam=" + partParam +
        '}';
  }
}