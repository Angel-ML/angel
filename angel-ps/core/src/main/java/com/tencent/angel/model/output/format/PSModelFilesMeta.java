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

package com.tencent.angel.model.output.format;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The meta data for all partition in a server(ps)
 */
public class PSModelFilesMeta {
  /**
   * Matrix id
   */
  private int matrixId;

  /**
   * Partition meta of all partitions in the server
   */
  private Map<Integer, ModelPartitionMeta> partMetas;

  /**
   * Create a empty ServerMatrixFilesMeta
   */
  public PSModelFilesMeta() {
    this(-1, new ConcurrentSkipListMap<Integer, ModelPartitionMeta>());
  }

  /**
   * Create a empty ServerMatrixFilesMeta for a matrix
   * @param matrixId matrix id
   */
  public PSModelFilesMeta(int matrixId) {
    this(matrixId, new ConcurrentSkipListMap<>());
  }

  /**
   * Create a  ServerMatrixFilesMeta for a matrix
   * @param matrixId matrix id
   * @param partMetas partition meta
   */
  public PSModelFilesMeta(int matrixId, Map<Integer, ModelPartitionMeta> partMetas) {
    this.matrixId = matrixId;
    this.partMetas = partMetas;
  }

  /**
   * Get all partitions meta
   * @return all partitions meta
   */
  public Map<Integer, ModelPartitionMeta> getPartMetas() {
    return partMetas;
  }

  /**
   * Add partition meta
   * @param partId partition id
   * @param meta partition meta
   */
  public void addPartitionMeta(int partId, ModelPartitionMeta meta) {
    partMetas.put(partId, meta);
  }

  /**
   * Get a partition meta
   * @param partId partition index
   * @return partition meta
   */
  public ModelPartitionMeta getPartitionMeta(int partId){
    return partMetas.get(partId);
  }

  /**
   * Write server matrix meta to output stream
   * @param output output stream
   * @throws IOException
   */
  public void write(DataOutputStream output) throws IOException {
    output.writeInt(matrixId);
    int size = partMetas.size();
    output.writeInt(size);
    for(Map.Entry<Integer, ModelPartitionMeta> partEntry : partMetas.entrySet()) {
      partEntry.getValue().write(output);
    }
  }

  /**
   * Read server matrix meta from input stream
   * @param input input stream
   * @throws IOException
   */
  public void read(DataInputStream input) throws IOException {
    matrixId = input.readInt();
    int size = input.readInt();
    partMetas = new HashMap<>(size);
    for(int i = 0; i < size; i++) {
      ModelPartitionMeta partMeta = new ModelPartitionMeta();
      partMeta.read(input);
      partMetas.put(partMeta.getPartId(), partMeta);
    }
  }

  @Override public String toString() {
    return "ps partMetas=[" + partMetasString() + "]}";
  }

  private String partMetasString() {
    if(partMetas == null || partMetas.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<Integer, ModelPartitionMeta> entry : partMetas.entrySet()) {
      if (first)
        first = false;
      else {
        sb.append(";");
      }
      sb.append("" + entry.getKey() + ":" + entry.getValue());
    }
    return sb.toString();
  }
}
