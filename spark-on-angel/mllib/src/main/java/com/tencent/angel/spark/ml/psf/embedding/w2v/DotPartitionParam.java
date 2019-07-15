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

package com.tencent.angel.spark.ml.psf.embedding.w2v;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class DotPartitionParam extends PartitionGetParam {

  int seed;
  int partitionId;
  int model;
  int[][] sentences;

  public DotPartitionParam(int matrixId,
                           int seed,
                           int partitionId,
                           int model,
                           PartitionKey pkey,
                           int[][] sentences) {
    super(matrixId, pkey);
    this.seed = seed;
    this.partitionId = partitionId;
    this.model = model;
    this.sentences = sentences;
  }

  public DotPartitionParam() {}

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(seed);
    buf.writeInt(partitionId);
    buf.writeInt(model);

    buf.writeInt(sentences.length);
    for (int a = 0; a < sentences.length; a ++) {
      buf.writeInt(sentences[a].length);
      for (int b = 0; b < sentences[a].length; b ++)
        buf.writeInt(sentences[a][b]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    this.seed = buf.readInt();
    this.partitionId = buf.readInt();
    this.model = buf.readInt();

    int length = buf.readInt();
    sentences = new int[length][];
    for (int a = 0; a < length; a++) {
      sentences[a] = new int[buf.readInt()];
      for (int b = 0; b < sentences[a].length; b ++)
        sentences[a][b] = buf.readInt();
    }
  }
}
