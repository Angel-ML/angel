package com.tencent.angel.spark.ml.tree.tree.split;

import java.io.Serializable;

// different types of tree node splits, enumerated by their complexity
public enum SplitType implements Serializable {
    SPLIT_POINT("SPLIT_POINT"),
    SPLIT_SET("SPLIT_SET");

    private final String type;

    SplitType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }
}
