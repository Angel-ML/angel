package com.tencent.angel.ml.treemodels.tree.regression;

import com.tencent.angel.ml.treemodels.param.GBDTParam;
import com.tencent.angel.ml.treemodels.tree.basic.SplitEntry;
import com.tencent.angel.ml.treemodels.tree.basic.TNode;


public class RegTNode extends TNode<RegTNodeStat> {

    public RegTNode(int nid, TNode parent, int numClass) {
        this(nid, parent, null, null, numClass);
    }

    public RegTNode(int nid, TNode parent, TNode left, TNode right, int numClass) {
        super(nid, parent, left, right);
        this.nodeStats = new RegTNodeStat[numClass == 2 ? 1 : numClass];
    }

    public void setGradStats(float sumGrad, float sumHess) {
        this.nodeStats[0] = new RegTNodeStat(sumGrad, sumHess);
    }

    public void setGradStats(float[] sumGrad, float[] sumHess) {
        for (int i = 0; i < this.nodeStats.length; i++) {
            this.nodeStats[i] = new RegTNodeStat(sumGrad[i], sumHess[i]);
        }
    }

    public float[] calcWeight(GBDTParam param) {
        float[] nodeWeights = new float[nodeStats.length];
        for (int i = 0; i < nodeStats.length; i++) {
            nodeWeights[i] = nodeStats[i].calcWeight(param);
        }
        return nodeWeights;
    }

    public float[] calcGain(GBDTParam param) {
        float[] nodeGains = new float[nodeStats.length];
        for (int i = 0; i < nodeStats.length; i++) {
            nodeGains[i] = nodeStats[i].calcGain(param);
        }
        return nodeGains;
    }

}
