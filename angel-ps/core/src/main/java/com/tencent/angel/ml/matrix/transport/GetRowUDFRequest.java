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

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.udf.getrow.PartitionGetRowParam;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GetRowUDFRequest extends PartitionRequest{
  private static final Log LOG = LogFactory.getLog(AggrRequest.class);
  private String getRowFuncClass;
  private PartitionGetRowParam partParam;
  
  public GetRowUDFRequest(ParameterServerId serverId, PartitionKey partKey, 
      String getRowFuncClass, PartitionGetRowParam partParam){
    super(serverId, 0, partKey);
    this.getRowFuncClass = getRowFuncClass;
    this.partParam = partParam;
  }
  
  public GetRowUDFRequest(){
    this(null, null, null, null);
  }
  
  
  @Override
  public int getEstimizeDataSize() {
    MatrixMeta meta =
        PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(partKey.getMatrixId());
    if (meta == null) {
      return 0;
    } else {
      RowType rowType = meta.getRowType();
      switch (rowType) {
        case T_DOUBLE_DENSE:
          return 8 * (partKey.getEndCol() - partKey.getStartCol());

        case T_INT_DENSE:
          return 4 * (partKey.getEndCol() - partKey.getStartCol());

        case T_FLOAT_DENSE:
          return 4 * (partKey.getEndCol() - partKey.getStartCol());

        case T_DOUBLE_SPARSE:
        case T_INT_SPARSE: {
          ServerRow row =
              PSAgentContext.get().getMatricesCache()
                  .getRowSplit(partKey.getMatrixId(), partKey, partParam.getRowIndex());
          if (row != null) {
            return row.bufferLen();
          } else {
            return 0;
          }
        }

        default:
          return 8 * (partKey.getEndCol() - partKey.getStartCol());
      }
    }
  }

  @Override
  public TransportMethod getType() {
    return TransportMethod.GETROW_UDF;
  }
  
  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if(getRowFuncClass != null){
      byte [] data = getRowFuncClass.getBytes();
      buf.writeInt(data.length);
      buf.writeBytes(data);
    }
    
    if(partParam != null){
      String partParamClassName = partParam.getClass().getName();
      byte [] data = partParamClassName.getBytes();
      buf.writeInt(data.length);
      buf.writeBytes(data);
      partParam.serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if(buf.isReadable()){
      int size = buf.readInt();
      byte [] data = new byte[size];
      buf.readBytes(data);
      getRowFuncClass = new String(data);
    }
    
    if(buf.isReadable()){
      int size = buf.readInt();
      byte [] data = new byte[size];
      buf.readBytes(data);
      String partParamClassName = new String(data);
      try {
        partParam = (PartitionGetRowParam) Class.forName(partParamClassName).newInstance();
        partParam.deserialize(buf);
      } catch (Exception e) {
        LOG.fatal("deserialize PartitionAggrParam falied, ", e);
      }
    }
  }

  @Override
  public int bufferLen() {
    int size = super.bufferLen();
    if(getRowFuncClass != null){
      size += 4;
      size += getRowFuncClass.toCharArray().length;
    }
    
    if(partParam != null){
      size += 4;
      size += partParam.bufferLen();
    }
    
    return size;
  }


  public String getGetRowFuncClass() {
    return getRowFuncClass;
  }

  public PartitionGetRowParam getPartParam() {
    return partParam;
  }

  public void setGetRowFuncClass(String getRowFuncClass) {
    this.getRowFuncClass = getRowFuncClass;
  }

  public void setPartParam(PartitionGetRowParam partParam) {
    this.partParam = partParam;
  }

  @Override
  public String toString() {
    return "GetRowUDFRequest [getRowFuncClass=" + getRowFuncClass + ", partParam=" + partParam
        + ", toString()=" + super.toString() + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getRowFuncClass == null) ? 0 : getRowFuncClass.hashCode());
    result = prime * result + ((partParam == null) ? 0 : partParam.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    GetRowUDFRequest other = (GetRowUDFRequest) obj;
    if (getRowFuncClass == null) {
      if (other.getRowFuncClass != null)
        return false;
    } else if (!getRowFuncClass.equals(other.getRowFuncClass))
      return false;
    if (partParam == null) {
      if (other.partParam != null)
        return false;
    } else if (!partParam.equals(other.partParam))
      return false;
    return true;
  }
}
