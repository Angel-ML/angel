package com.tencent.angel.graph.model.neighbor.samplewithtrunc;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.common.psf.param.LongKeysGetParam;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;

import java.util.ArrayList;
import java.util.List;

public class SampleTruncParam extends LongKeysGetParam {
    private final boolean isTrunc;
    private final int truncLen;
    private final boolean dynamicInitNeighbor;

    public SampleTruncParam(int matrixId, long[] nodeIds, boolean isTrunc, int truncLen) {
        super(matrixId, nodeIds);
        this.isTrunc = isTrunc;
        this.truncLen = truncLen;
        this.dynamicInitNeighbor = false;
    }

    public SampleTruncParam(int matrixId, long[] nodeIds, boolean isTrunc, int truncLen, boolean dynamicInitNeighbor) {
        super(matrixId, nodeIds);
        this.isTrunc = isTrunc;
        this.truncLen = truncLen;
        this.dynamicInitNeighbor = dynamicInitNeighbor;
    }

    public SampleTruncParam() {this(-1, null, false, -1, false);}

    public boolean getIsTrunc() {return isTrunc;}

    public int getTruncLen() {return truncLen;}

    public boolean getDynamicInitNeighbor() {return dynamicInitNeighbor;}

    @Override
    public List<PartitionGetParam> split() {
        // Get matrix meta
        MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
        PartitionKey[] parts = meta.getPartitionKeys();

        // Split
        KeyPart[] nodeIdsParts = RouterUtils.split(meta, 0, nodeIds, false);

        // Generate Part psf get param
        List<PartitionGetParam> partParams = new ArrayList<>(parts.length);
        assert parts.length == nodeIdsParts.length;
        for (int i = 0; i < parts.length; i++) {
            if (nodeIdsParts[i] != null && nodeIdsParts[i].size() > 0) {
                partParams.add(new PartSampleTruncParam(matrixId, parts[i], nodeIdsParts[i], isTrunc, truncLen, dynamicInitNeighbor));
            }
        }

        return partParams;
    }

}