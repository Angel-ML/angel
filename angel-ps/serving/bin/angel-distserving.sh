#!/usr/bin/env bash

if [ $# -eq 0 -o $1 = "-h" -o $1 = "--help" ]; then
    echo "Usage: distserving load|unload|predict -D..."
    exit
fi

DEFAULT_LIBEXEC_DIR=${HADOOP_HOME}/libexec
HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
. ${HADOOP_LIBEXEC_DIR}/hadoop-config.sh

CLASSPATHLOC=".:"
for jar in ${ANGEL_HOME}/lib/*.jar; do
    if [ "$CLASSPATHLOC" ]; then
         CLASSPATHLOC=${CLASSPATHLOC}:${jar}
    else
         CLASSPATHLOC=${jar}
    fi
done

# add jar and log path to CLASSPATH, so that java can find jar and log config
export CLASSPATH=${CLASSPATH}:${CLASSPATHLOC}

if [ $1 = "-load" -o $1 = "load" ]; then
    MainClass=com.tencent.angel.serving.laucher.DistributeModelLoader
    Opts="-Daction.type=serving -Dangel.serving.model.load.type=load -Dangel.output.path.deleteonexist=true"
elif [ $1 = "-unload" -o $1 = "unload" ]; then
    MainClass=com.tencent.angel.serving.laucher.DistributeModelLoader
    Opts="-Daction.type=serving -Dangel.serving.model.load.type=unload -Dangel.output.path.deleteonexist=true"
elif [ $1 = "-predict" -o $1 = "predict" ]; then
    MainClass=com.tencent.angel.serving.laucher.ServingPredictClient
    Opts="-Daction.type=serving -Dangel.output.path.deleteonexist=true"
else
    echo "parameter error, only model and predict are allowed!"
    echo "Usage: distserving load|unload|predict -D..."
    exit
fi

shift

scala -cp ${CLASSPATH} ${MainClass} ${Opts} "$@"
