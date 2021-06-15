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
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Update udf rpc request.
 */
public class UpdateUDFRequest extends RequestData {
  private static final Log LOG = LogFactory.getLog(UpdateUDFRequest.class);
  /**
   * update udf function class name
   */
  private String updaterFuncClass;

  /**
   * the partition parameter of the update udf
   */
  private PartitionUpdateParam partParam;

  /**
   * Create a new UpdaterRequest.
   *
   * @param updaterFuncClass update udf function class name
   * @param partParam        the partition parameter of the update udf
   */
  public UpdateUDFRequest(String updaterFuncClass,
    PartitionUpdateParam partParam) {
    this.updaterFuncClass = updaterFuncClass;
    this.partParam = partParam;
  }

  /**
   * Create a new UpdaterRequest.
   */
  public UpdateUDFRequest() {
    this( null, null);
  }

  @Override public void serialize(ByteBuf buf) {
    ByteBufSerdeUtils.serializeUTF8(buf, updaterFuncClass);
    ByteBufSerdeUtils.serializeObject(buf, partParam);
  }

  @Override public void deserialize(ByteBuf buf) {
    int readerIndex = buf.readerIndex();
    updaterFuncClass = ByteBufSerdeUtils.deserializeUTF8(buf);
    partParam = (PartitionUpdateParam) ByteBufSerdeUtils.deserializeObject(buf);
    requestSize = buf.readerIndex() - readerIndex;
  }

  @Override public int bufferLen() {
    return ByteBufSerdeUtils.serializedUTF8Len(updaterFuncClass)
        + ByteBufSerdeUtils.serializedObjectLen(partParam);
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

  @Override public String toString() {
    return "UpdaterRequest [updaterFuncClass=" + updaterFuncClass + ", partParam=" + partParam
      + ", toString()=" + super.toString() + "]";
  }
}