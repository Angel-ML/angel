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

package com.tencent.angel.ml.matrix.udf.aggr;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.Serialize;
import io.netty.buffer.ByteBuf;

/**
 * The parameter of Partition aggregation.
 */
public class PartitionAggrParam implements Serialize{
  private int matrixId;
  private PartitionKey partKey;

  /**
   * Create a new parameter.
   *
   * @param matrixId the matrix id
   * @param partKey  the part key
   */
  public PartitionAggrParam(int matrixId, PartitionKey partKey){
    this.matrixId = matrixId;
    this.partKey = partKey;
  }

  /**
   *  Create a new parameter by default.
   */
  public PartitionAggrParam(){
    this(0, null);
  }
  
  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(matrixId);
    if(partKey != null){
      partKey.serialize(buf);
    }
  }
  
  @Override
  public void deserialize(ByteBuf buf) {
    matrixId = buf.readInt();
    if(buf.isReadable()){
      if(partKey == null){
        partKey = new PartitionKey();
      }
      
      partKey.deserialize(buf);
    }
  }
  
  @Override
  public int bufferLen() {
    return 4 + ((partKey != null) ? partKey.bufferLen() : 0);
  }

  /**
   * Gets matrix id.
   *
   * @return the matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Gets part key.
   *
   * @return the part key
   */
  public PartitionKey getPartKey() {
    return partKey;
  }

  /**
   * Sets matrix id.
   *
   * @param matrixId the matrix id
   */
  public void setMatrixId(int matrixId) {
    this.matrixId = matrixId;
  }

  /**
   * Sets part key.
   *
   * @param partKey the part key
   */
  public void setPartKey(PartitionKey partKey) {
    this.partKey = partKey;
  }
  
}
