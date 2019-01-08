package com.tencent.angel.spark.ml.embedding;

import java.io.Serializable;

public class EmbeddingConf implements Serializable {
    // possible values of each option is included in []

    // path of training data
    public static final String INPUTPATH = "input"; // [inputpath]
    // path of output, i.e., the embedding vectors
    public static final String OUTPUTPATH = "output"; // [outputpath]
    // the dimension of embedding vectors
    public static final String EMBEDDINGDIM = "embedding"; // [100,]
    // number of negative samples
    public static final String NEGATIVESAMPLENUM = "negative"; // [5,]
    // window size for word2vec
    public static final String WINDOWSIZE = "window"; // [5,]
    // number of epochs to train
    public static final String NUMEPOCH = "epoch"; // [10,]
    // learning rate
    public static final String STEPSIZE = "stepsize"; // [0.1,]
    // batch size
    public static final String BATCHSIZE = "batchSize"; // [10,]
    // number of partitions for the storage of parameters on parameter server
    public static final String NUMPARTITIONS = "numParts"; // [10, ]
    // number of vertex in each row in PS, i.e., (context1, word1, contex2, ..., wordn)
    public static final String NUMNODEPERROW = "numNodePerRow"; // [10000,]
    // whether use subsampling for frequent vertice
    public static final String WITHSUBSAMPLE = "subSample"; // [true, false]
    // whether use remapping for vertex
    public static final String WITHREMAPPING = "remapping"; // [true, false]
    // model type, i.e., cbow and sgns
    public static final String MODELTYPE = "modelType"; // [sgns, cbow]
    // interval of checkpoint
    public static final String CHECKPOINTINTERVAL = "interval"; // [10,]
    // the stop probablity in rooted pagerank. The bigger, the longer the path is.
    public static final String ROOTEDPAGERANKPRP = "rprPrp"; //[0.5]
    public static final String SAMPLERNAME = "sampler"; // [rootedPageRank,]

}