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

import com.tencent.angel.ml.matrix.udf.getrow.PartitionGetRowResult;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class GetRowUDFResponse extends Response{
  private static final Log LOG = LogFactory.getLog(AggrResponse.class);
  private PartitionGetRowResult partResult;
  
  public GetRowUDFResponse(PartitionGetRowResult partResult){
   this.partResult = partResult;
  }

  public GetRowUDFResponse(){
    this(null);
  }
  
  public PartitionGetRowResult getPartResult() {
    return partResult;
  }

  public void setPartResult(PartitionGetRowResult partResult) {
    this.partResult = partResult;
  }
  
  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if(partResult != null){
      byte [] data = partResult.getClass().getName().getBytes();
      buf.writeInt(data.length);
      buf.writeBytes(data);
      
      partResult.serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if(buf.isReadable()){
      int size = buf.readInt();
      byte [] data = new byte[size];
      buf.readBytes(data);
      String partResultClassName = new String(data);
      try {
        partResult = (PartitionGetRowResult) Class.forName(partResultClassName).newInstance();
        partResult.deserialize(buf);
      } catch (Exception e) {
        LOG.fatal("deserialize PartitionAggrResult falied, ", e);
      }
    }
  }

  @Override
  public int bufferLen() {
    int size = super.bufferLen();
    if(partResult != null){
      size += 4;
      size += partResult.bufferLen();
    }
    
    return size;
  }

  @Override
  public String toString() {
    return "AggrResponse [partResult=" + partResult + ", toString()=" + super.toString() + "]";
  }
}
