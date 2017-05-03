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
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrParam;
import com.tencent.angel.ps.ParameterServerId;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AggrRequest extends PartitionRequest{
  private static final Log LOG = LogFactory.getLog(AggrRequest.class);
  private String aggrFuncClass;
  private PartitionAggrParam partParam;
  
  public AggrRequest(ParameterServerId serverId, PartitionKey partKey, 
      String aggrFuncClass, PartitionAggrParam partParam){
    super(serverId, 0, partKey);
    this.aggrFuncClass = aggrFuncClass;
    this.partParam = partParam;
  }
  
  public AggrRequest(){
    this(null, null, null, null);
  }
  
  @Override
  public int getEstimizeDataSize() {
    return 4;
  }

  @Override
  public TransportMethod getType() {
    return TransportMethod.AGGR;
  }
  
  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if(aggrFuncClass != null){
      byte [] data = aggrFuncClass.getBytes();
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
      aggrFuncClass = new String(data);
    }
    
    if(buf.isReadable()){
      int size = buf.readInt();
      byte [] data = new byte[size];
      buf.readBytes(data);
      String partParamClassName = new String(data);
      try {
        partParam = (PartitionAggrParam) Class.forName(partParamClassName).newInstance();
        partParam.deserialize(buf);
      } catch (Exception e) {
        LOG.fatal("deserialize PartitionAggrParam falied, ", e);
      }
    }
  }

  @Override
  public int bufferLen() {
    int size = super.bufferLen();
    if(aggrFuncClass != null){
      size += 4;
      size += aggrFuncClass.toCharArray().length;
    }
    
    if(partParam != null){
      size += 4;
      size += partParam.bufferLen();
    }
    
    return size;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((aggrFuncClass == null) ? 0 : aggrFuncClass.hashCode());
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
    AggrRequest other = (AggrRequest) obj;
    if (aggrFuncClass == null) {
      if (other.aggrFuncClass != null)
        return false;
    } else if (!aggrFuncClass.equals(other.aggrFuncClass))
      return false;
    if (partParam == null) {
      if (other.partParam != null)
        return false;
    } else if (!partParam.equals(other.partParam))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "AggrRequest [aggrFuncClass=" + aggrFuncClass + ", partParam=" + partParam
        + ", toString()=" + super.toString() + "]";
  }

  public String getAggrFuncClass() {
    return aggrFuncClass;
  }

  public PartitionAggrParam getPartParam() {
    return partParam;
  }

  public void setAggrFuncClass(String aggrFuncClass) {
    this.aggrFuncClass = aggrFuncClass;
  }

  public void setPartParam(PartitionAggrParam partParam) {
    this.partParam = partParam;
  }

}
