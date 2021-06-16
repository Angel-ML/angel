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
package com.tencent.angel.graph.psf.kcore.readTag;

import com.tencent.angel.common.collections.DynamicLongArray;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongIntRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;

import java.util.List;

public class ReadTag extends GetFunc {

    public ReadTag(int matrixId, long[] nodes, int rowId) {
        this(new ReadTagParam(matrixId, nodes, rowId));
    }

    public ReadTag(ReadTagParam param) {
        super(param);
    }

    public ReadTag() {
        this(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        GeneralPartGetParam param = (GeneralPartGetParam) partParam;
        KeyPart keyPart = param.getIndicesPart();
        long[] nodeIds = ((ILongKeyPartOp) keyPart).getKeys();
        ServerLongIntRow row = GraphMatrixUtils.getPSLongKeyIntRow(psContext, param, param.getRowId());

        DynamicLongArray nodes = new DynamicLongArray(nodeIds.length);
        for (int i = 0; i < nodeIds.length; i++) {
            long nodeId = nodeIds[i];
            if (row.get(nodeId) > 0) {
                nodes.add(nodeId);
            }
        }
        return new PartReadTagResult(nodes.getData());
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        DynamicLongArray re = new DynamicLongArray(0);
        for (PartitionGetResult result: partResults) {
            PartReadTagResult partResult = (PartReadTagResult) result;
            long[] nodeIds = partResult.getNodes();
            for (long id: nodeIds) {
                re.add(id);
            }
        }
        return new ReadTagResult(re.getData());
    }
}
