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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;

import java.util.ArrayList;
import java.util.List;

public class ReadTagParam extends GetParam {

    private final long[] nodeIds;

    private final int rowId;

    public ReadTagParam(int matrixId, long[] nodeIds, int rowId) {
        super(matrixId);
        this.nodeIds = nodeIds;
        this.rowId = rowId;
    }

    public ReadTagParam() {
        this(-1, null, 0);
    }

    public long[] getNodeIds() {
        return nodeIds;
    }

    public int getRowId() {
        return rowId;
    }

    @Override
    public List<PartitionGetParam> split() {
        MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
        PartitionKey[] parts = meta.getPartitionKeys();

        KeyPart[] nodeIdsParts = RouterUtils.split(meta, rowId, nodeIds, false);

        List<PartitionGetParam> partParams = new ArrayList<>(parts.length);
        assert parts.length == nodeIdsParts.length;
        for (int i = 0; i < parts.length; i++) {
            if (nodeIdsParts[i] != null && nodeIdsParts[i].size() > 0) {
                partParams.add(new GeneralPartGetParam(matrixId, parts[i], nodeIdsParts[i]));
            }
        }
        return partParams;
    }

}
