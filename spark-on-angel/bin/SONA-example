#!/bin/bash

source ./spark-on-angel-env.sh

${SPARK_HOME}/bin/spark-submit \
    --master yarn-cluster \
    --conf spark.ps.jars=${SONA_ANGEL_JARS} \
    --conf spark.ps.instances=2 \
    --conf spark.ps.cores=2 \
    --conf spark.ps.memory=2g \
    --jars ${SONA_SPARK_JARS} \
    --name "PageRank-spark-on-angel" \
    --driver-memory 1g \
    --num-executors 2 \
    --executor-cores 2 \
    --executor-memory 2g \
    --class com.tencent.angel.spark.examples.cluster.PageRankExample \
    ./../lib/spark-on-angel-examples-${ANGEL_VERSION}.jar \
    input:${ANGEL_HDFS_HOME}/data/bc/edge \
    output:${ANGEL_HDFS_HOME}/output \
    resetProp:0.15 \
