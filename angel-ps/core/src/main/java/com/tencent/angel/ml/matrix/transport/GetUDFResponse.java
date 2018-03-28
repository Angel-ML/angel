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
 *
 */

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The result of get udf request.
 */
public class GetUDFResponse extends Response {
  private static final Log LOG = LogFactory.getLog(GetUDFResponse.class);
  /** the get result of the matrix partition */
  private PartitionGetResult partResult;

  /**
   * Create a new GetUDFResponse
   * @param responseType response type
   * @param detail detail failed message if the response is not success
   * @param partResult result
   */
  public GetUDFResponse(ResponseType responseType, String detail, PartitionGetResult partResult) {
    super(responseType, detail);
    this.partResult = partResult;
  }

  /**
   * Create a new GetUDFResponse
   * @param responseType response type
   * @param partResult result
   */
  public GetUDFResponse(ResponseType responseType, PartitionGetResult partResult) {
    this(responseType, null, partResult);
  }

  /**
   * Create a new GetUDFResponse
   * @param responseType response type
   * @param detail detail failed message if the response is not success
   */
  public GetUDFResponse(ResponseType responseType, String detail) {
    this(responseType, detail, null);
  }

  /**
   * Create a new GetUDFResponse.
   */
  public GetUDFResponse() {
    this(ResponseType.SUCCESS, null, null);
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

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (partResult != null) {
      byte[] data = partResult.getClass().getName().getBytes();
      buf.writeInt(data.length);
      buf.writeBytes(data);

      partResult.serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if (buf.isReadable()) {
      int size = buf.readInt();
      byte[] data = new byte[size];
      buf.readBytes(data);
      String partResultClassName = new String(data);
      try {
        partResult = (PartitionGetResult) Class.forName(partResultClassName).newInstance();
        partResult.deserialize(buf);
      } catch (Exception e) {
        LOG.fatal("deserialize PartitionAggrResult falied, ", e);
      }
    }
  }

  @Override
  public int bufferLen() {
    int size = super.bufferLen();
    if (partResult != null) {
      size += 4;
      size += partResult.getClass().getName().getBytes().length;
      size += partResult.bufferLen();
    }

    return size;
  }

  @Override
  public String toString() {
    return "GetUDFResponse [partResult=" + partResult + ", toString()=" + super.toString() + "]";
  }

  @Override
  public void clear() {
    setPartResult(null);
  }
}
