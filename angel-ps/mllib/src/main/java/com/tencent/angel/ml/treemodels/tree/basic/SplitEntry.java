package com.tencent.angel.ml.treemodels.tree.basic;

import com.tencent.angel.ml.treemodels.tree.regression.GradPair;

public class SplitEntry {
    private int fid;  // feature index used to split
    private float fvalue;  // feature value used to split
    private float lossChg;  // loss change after split this node
    private GradPair leftGradPair;  // grad pair of left child
    private GradPair rightGradPair; // grad pair of right child

    public SplitEntry(int fid, float fvalue, float lossChg) {
        this.fid = fid;
        this.fvalue = fvalue;
        this.lossChg = lossChg;
    }

    public SplitEntry() {
        this(-1, 0.0f, 0.0f);
    }

    /**
     * decides whether we can replace current entry with the given statistics This function gives
     * better priority to lower index when loss_chg == new_loss_chg. Not the best way, but helps to
     * give consistent result during multi-thread execution.
     *
     * @param newLossChg the new loss change
     * @param splitFeature the split index
     * @return the boolean whether the proposed split is better and can replace current split
     */
    public boolean NeedReplace(float newLossChg, int splitFeature) {
        if (this.fid <= splitFeature) {
            return newLossChg > this.lossChg;
        } else {
            return !(this.lossChg > newLossChg);
        }
    }

    /**
     * Update the split entry, replace it if e is better.
     *
     * @param e candidate split solution
     * @return the boolean whether the proposed split is better and can replace current split
     */
    public boolean update(SplitEntry e) {
        if (this.NeedReplace(e.lossChg, e.getFid())) {
            this.lossChg = e.lossChg;
            this.fid = e.fid;
            this.fvalue = e.fvalue;
            this.leftGradPair = e.leftGradPair;
            this.rightGradPair = e.rightGradPair;
            return true;
        } else {
            return false;
        }
    }

    public boolean update(float newLossChg, int splitFeature, float newSplitValue) {
        if (this.NeedReplace(newLossChg, splitFeature)) {
            this.lossChg = newLossChg;
            this.fid = splitFeature;
            this.fvalue = newSplitValue;
            return true;
        } else {
            return false;
        }
    }

    public int getFid() {
        return fid;
    }

    public float getFvalue() {
        return fvalue;
    }

    public float getLossChg() {
        return lossChg;
    }

    public GradPair getLeftGradPair() {
        return leftGradPair;
    }

    public GradPair getRightGradPair() {
        return rightGradPair;
    }

    public void setFid(int fid) {
        this.fid = fid;
    }

    public void setFvalue(float fvalue) {
        this.fvalue = fvalue;
    }

    public void setLossChg(float lossChg) {
        this.lossChg = lossChg;
    }

    public void setLeftGradPair(GradPair leftGradPair) {
        this.leftGradPair = leftGradPair;
    }

    public void setRightGradPair(GradPair rightGradPair) {
        this.rightGradPair = rightGradPair;
    }
}
