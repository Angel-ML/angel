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

package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.shorts.Short2IntMap;
import it.unimi.dsi.fastutil.shorts.Short2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class CSRPartUpdateParam extends PartitionUpdateParam {
  private final static Log LOG = LogFactory.getLog(CSRPartUpdateParam.class);

  Short2IntOpenHashMap[] updates;
  ByteBuf buf;

  public CSRPartUpdateParam(int matId, PartitionKey pkey, Short2IntOpenHashMap[] updates) {
    super(matId, pkey);
    this.updates = updates;
  }

  public CSRPartUpdateParam() {
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    int w = getPartKey().getStartRow();
    for (int i = 0; i < updates.length; i++) {
      if (updates[i] != null) {
        buf.writeInt(w + i);
        Short2IntOpenHashMap map = updates[i];
        buf.writeShort(map.size());
        ObjectIterator<Short2IntMap.Entry> iter = map.short2IntEntrySet().fastIterator();
        while (iter.hasNext()) {
          Short2IntMap.Entry entry = iter.next();
          buf.writeShort(entry.getShortKey());
          buf.writeInt(entry.getIntValue());
        }
      }
    }

  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    this.buf = buf.duplicate();
    this.buf.retain();

  }

  @Override public int bufferLen() {
    int len = 0;
    for (int i = 0; i < updates.length; i++) {
      if (updates[i] != null)
        len += updates[i].size() * 6;
    }
    return super.bufferLen() + len;
  }

}
