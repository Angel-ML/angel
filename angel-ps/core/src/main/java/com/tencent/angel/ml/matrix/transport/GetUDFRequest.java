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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
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
   * @param partKey      matrix partition key
   * @param getFuncClass udf class name
   * @param partParam    partition parameter of the udf
   */
  public GetUDFRequest(PartitionKey partKey, String getFuncClass,
    PartitionGetParam partParam) {
    super(0, partKey);
    this.getFuncClass = getFuncClass;
    this.partParam = partParam;
  }

  /**
   * Create a new GetUDFRequest.
   */
  public GetUDFRequest() {
    this(null, null, null);
  }

  @Override public int getEstimizeDataSize() {
    MatrixMeta meta =
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(partKey.getMatrixId());
    if (meta == null) {
      return 0;
    } else {
      RowType rowType = meta.getRowType();
      switch (rowType) {
        case T_DOUBLE_DENSE:
          return 8 * ((int) partKey.getEndCol() - (int) partKey.getStartCol());

        case T_INT_DENSE:
          return 4 * ((int) partKey.getEndCol() - (int) partKey.getStartCol());

        case T_FLOAT_DENSE:
          return 4 * ((int) partKey.getEndCol() - (int) partKey.getStartCol());

        default:
          return 0;
      }
    }
  }

  @Override public TransportMethod getType() {
    return TransportMethod.GET_UDF;
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

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;

    GetUDFRequest that = (GetUDFRequest) o;
    return getFuncClass.equals(that.getFuncClass) && partParam.equals(that.partParam);
  }

  @Override public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + getFuncClass.hashCode();
    result = 31 * result + partParam.hashCode();
    return result;
  }

  @Override public String toString() {
    return "GetUDFRequest{" + "getFuncClass='" + getFuncClass + '\'' + ", partParam=" + partParam
      + "} " + super.toString();
  }
}
