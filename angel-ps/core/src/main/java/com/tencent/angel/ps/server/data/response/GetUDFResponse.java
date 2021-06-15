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


package com.tencent.angel.ps.server.data.response;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The result of get udf request.
 */
public class GetUDFResponse extends ResponseData {
  private static final Log LOG = LogFactory.getLog(GetUDFResponse.class);
  /**
   * the get result of the matrix partition
   */
  private PartitionGetResult partResult;

  /**
   * Create a new GetUDFResponse
   *
   * @param partResult   result
   */
  public GetUDFResponse(PartitionGetResult partResult) {
    this.partResult = partResult;
  }

  /**
   * Create a new GetUDFResponse.
   */
  public GetUDFResponse() {
    this(null);
  }

  /**
   * Get the result of the matrix partition.
   *
   * @return PartitionGetResult the result of the matrix partition
   */
  public PartitionGetResult getPartResult() {
    return partResult;
  }

  /**
   * Set the result of the matrix partition.
   *
   * @param partResult the result of the matrix partition
   */
  public void setPartResult(PartitionGetResult partResult) {
    this.partResult = partResult;
  }

  @Override public void serialize(ByteBuf buf) {
    ByteBufSerdeUtils.serializeObject(buf, partResult);
  }

  @Override public void deserialize(ByteBuf buf) {
    partResult = (PartitionGetResult) ByteBufSerdeUtils.deserializeObject(buf);
  }

  @Override public int bufferLen() {
    return partResult.bufferLen();
  }

  @Override public String toString() {
    return "GetUDFResponse [partResult=" + partResult + ", toString()=" + super.toString() + "]";
  }
}
