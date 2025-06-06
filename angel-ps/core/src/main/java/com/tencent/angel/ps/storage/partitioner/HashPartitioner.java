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
   * Total partition number
   */
  protected int totalPartNum;

  /**
   * Part id to ps id map
   */
  protected int[] partId2serverIndex;

  private volatile boolean splitted = false;

  @Override
  public void init(MatrixContext mContext, AMContext context) {
    this.mContext = mContext;
    this.context = context;
    this.conf = context.getConf();

    // Partition number use set
    totalPartNum = mContext.getPartitionNum();
    int psNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);

    if(totalPartNum > 0) {
      // Use total partition number if set
      LOG.info("Use set partition number=" + totalPartNum + ", ps number=" + psNum);
      mContext.setPartitionNum(totalPartNum);
    } else {
      // Use partition number per server
      totalPartNum =
              conf.getInt(AngelConf.ANGEL_MODEL_PARTITIONER_PARTITION_NUM_PERSERVER,
                      AngelConf.DEFAULT_ANGEL_MODEL_PARTITIONER_PARTITION_NUM_PERSERVER) * psNum;
    }

    if(totalPartNum <= 0) {
      throw new AngelException("total partition number " + totalPartNum + " is not valid");
    }

    this.partId2serverIndex = new int[totalPartNum];
  }

  @Override
  public List<PartitionMeta> getPartitions() {
    int psNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
    List<PartitionMeta> partitions = new ArrayList<>(totalPartNum);
    int matrixId = mContext.getMatrixId();
    for(int partId = 0; partId < totalPartNum; partId++) {
      PartitionMeta partMeta = new PartitionMeta(matrixId, partId,
         0, mContext.getRowNum(), partId, partId + 1);
      partitions.add(partMeta);
      partId2serverIndex[partId] = partId % psNum;
    }

    splitted = true;
    return partitions;
  }

  @Override
  public int assignPartToServer(int partId) {
    if(splitted) {
      return partId2serverIndex[partId];
    } else {
      int serverNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
      return partId % serverNum;
    }
  }

  @Override
  public PartitionType getPartitionType() {
    return PartitionType.HASH_PARTITION;
  }
}