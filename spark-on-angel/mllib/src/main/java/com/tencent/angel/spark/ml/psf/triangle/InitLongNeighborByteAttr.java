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

package com.tencent.angel.spark.ml.psf.triangle;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public class InitLongNeighborByteAttr extends UpdateFunc {

    private static final Logger LOG = LoggerFactory.getLogger(InitLongNeighborByteAttr.class);

    /**
     * Create a new UpdateParam
     *
     * @param param
     */
    public InitLongNeighborByteAttr(UpdateParam param) {
        super(param);
    }

    public InitLongNeighborByteAttr() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        PartInitNeighborAttrParam param = (PartInitNeighborAttrParam) partParam;
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        RowBasedPartition part = (RowBasedPartition) matrix
                .getPartition(partParam.getPartKey().getPartitionId());
        ServerLongAnyRow row = (ServerLongAnyRow) part.getRow(0);

        ObjectIterator<Long2ObjectMap.Entry<NeighborsAttrsElement>> iter =
                param.getNodeId2Neighbors().long2ObjectEntrySet().iterator();

        row.startWrite();
        try {
            while (iter.hasNext()) {
                Long2ObjectMap.Entry<NeighborsAttrsElement> entry = iter.next();
                NeighborsAttrsElement elem = entry.getValue();
                if (elem == null) {
                    row.set(entry.getLongKey(), null);
                } else {
                    // compress the neighbor IDs
                    try {
                        byte[] compressed = Snappy.compress(elem.getNeighborIds());
                        NeighborsAttrsCompressedElement compressedElement = new NeighborsAttrsCompressedElement(compressed, elem.getAttrs());
                        row.set(entry.getLongKey(), compressedElement);
                    } catch (IOException e) {
                        LOG.error("save neighbor table error!", e);
                        row.set(entry.getLongKey(), null);
                    }
                }

            }
        } finally {
            row.endWrite();
        }
    }


}
