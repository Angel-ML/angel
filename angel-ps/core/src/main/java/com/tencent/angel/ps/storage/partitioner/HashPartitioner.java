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
package com.tencent.angel.ps.storage.partitioner;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.PartitionMeta;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Base class of range partitioner
 */
public class HashPartitioner implements Partitioner {

  private static final Log LOG = LogFactory.getLog(HashPartitioner.class);
  /**
   * Matrix context
   */
  protected MatrixContext mContext;

  /**
   * AMContext context
   */
  protected AMContext context;

  /**
   * Application configuration
   */
  protected Configuration conf;

  /**
   * Partition number per server
   */
  protected int partNumPerServer;

  /**
   * Part id to ps id map
   */
  protected int[] partId2serverIndex;

  @Override
  public void init(MatrixContext mContext, AMContext context) {
    this.mContext = mContext;
    this.context = context;
    this.conf = context.getConf();

    // Partition number use set
    int totalPartNum = mContext.getPartitionNum();
    int psNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);

    if(totalPartNum > 0) {
      // Use total partition number if set
      partNumPerServer = Math.max(totalPartNum / psNum, 1);
      LOG.info("Use set partition number=" + totalPartNum
          + ", ps number=" + psNum
          + ", partition per server=" + partNumPerServer
          + ", total partition number adjust to " + partNumPerServer * psNum);
      mContext.setPartitionNum(partNumPerServer * psNum);
    } else {
      // Use partition number per server
      partNumPerServer =
          conf.getInt(AngelConf.ANGEL_MODEL_PARTITIONER_PARTITION_NUM_PERSERVER,
              AngelConf.DEFAULT_ANGEL_MODEL_PARTITIONER_PARTITION_NUM_PERSERVER);
    }

    if(partNumPerServer <= 0) {
      throw new AngelException("Partition number per server " + partNumPerServer + " is not valid");
    }

    this.partId2serverIndex = new int[psNum * partNumPerServer];
  }

  @Override
  public List<PartitionMeta> getPartitions() {
    int psNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
    List<PartitionMeta> partitions = new ArrayList<>(psNum * partNumPerServer);
    int matrixId = mContext.getMatrixId();
    for(int psIndex = 0; psIndex < psNum; psIndex++) {
      for(int partIndex = 0; partIndex < partNumPerServer; partIndex++) {
        int partId = psIndex * partNumPerServer + partIndex;
        PartitionMeta partMeta = new PartitionMeta(matrixId, partId,
            0, mContext.getRowNum(), partId, partId + 1);
        partitions.add(partMeta);
        partId2serverIndex[partId] = partId % psNum;
      }
    }

    return partitions;
  }

  @Override
  public int assignPartToServer(int partId) {
    return partId2serverIndex[partId];
  }

  @Override
  public PartitionType getPartitionType() {
    return PartitionType.HASH_PARTITION;
  }
}