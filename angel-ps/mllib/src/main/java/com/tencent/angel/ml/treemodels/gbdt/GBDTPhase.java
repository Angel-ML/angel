package com.tencent.angel.ml.treemodels.gbdt;

public enum GBDTPhase {
    NEW_TREE("NEW_TREE"),
    CHOOSE_ACTIVE("CHOOSE_ACTIVE"),
    RUN_ACTIVE("RUN_ACTIVE"),
    FIND_SPLIT("FIND_SPLIT"),
    AFTER_SPLIT("AFTER_SPLIT"),
    FINISH_TREE("FINISH_TREE"),
    FINISHED("FINISHED");

    private final String phase;

    private GBDTPhase(String phase) {
        this.phase = phase;
    }

    @Override
    public String toString() {
        return phase;
    }
}
