#!/bin/bash

# Before run Spark on Angel application, you must follow the steps:
# 1. confirm Hadoop and Spark have ready in your environment
# 2. unzip angel-<version>-bin.zip to local directory
# 3. upload angel-<version>-bin directory to HDFS
# 4. set the following variables, ANGEL_HOME, ANGEL_HDFS_HOME, ANGEL_VERSION

export ANGEL_HOME=<Angel/Home>
export ANGEL_HDFS_HOME=<Angel/HDFS/Home>
export ANGEL_VERSION=<Version>

scala_jar=scala-library-2.11.8.jar
external_jar=fastutil-7.1.0.jar,htrace-core-2.05.jar,sizeof-0.3.0.jar,kryo-shaded-4.0.0.jar,minlog-1.3.0.jar,sketches-core-0.8.1.jar,memory-0.8.1.jar,commons-pool-1.6.jar
angel_ps_jar=angel-ps-core-${ANGEL_VERSION}.jar,angel-ps-mllib-${ANGEL_VERSION}.jar,angel-ps-examples-${ANGEL_VERSION}.jar,angel-ps-psf-${ANGEL_VERSION}.jar

sona_jar=spark-on-angel-core-${ANGEL_VERSION}.jar,spark-on-angel-mllib-${ANGEL_VERSION}.jar
sona_psf_jar=spark-on-angel-mllib-${ANGEL_VERSION}-ps.jar,spark-on-angel-examples-${ANGEL_VERSION}-ps.jar

dist_jar=${external_jar},${angel_ps_jar},${sona_psf_jar},${scala_jar}
local_jar=${external_jar},${angel_ps_jar},${sona_jar}

for f in `echo $dist_jar | awk -F, '{for(i=1; i<=NF; i++){ print $i}}'`; do
	jar=${ANGEL_HDFS_HOME}/lib/${f}
    if [ "$SONA_ANGEL_JARS" ]; then
        SONA_ANGEL_JARS=$SONA_ANGEL_JARS,$jar
    else
        SONA_ANGEL_JARS=$jar
    fi
done
echo SONA_ANGEL_JARS: $SONA_ANGEL_JARS
export SONA_ANGEL_JARS 


for f in `echo $local_jar | awk -F, '{for(i=1; i<=NF; i++){ print $i}}'`; do
	jar=${ANGEL_HOME}/lib/${f}
    if [ "$SONA_SPARK_JARS" ]; then
        SONA_SPARK_JARS=$SONA_SPARK_JARS,$jar
    else
        SONA_SPARK_JARS=$jar
    fi
done
echo SONA_SPARK_JARS: $SONA_SPARK_JARS
export SONA_SPARK_JARS

command="$1 --conf spark.ps.jars=$SONA_ANGEL_JARS --jars $SONA_SPARK_JARS"

is_first_parama=true
is_jars=false
for param in "$@"; do
    if $is_first_param ; then 
        is_first_param=false
    else 
        if [[ $param == "--jars" ]]; then
            is_jars=true
        elif $is_jars && [[ $param != "\-\-*" ]] ; then
            param=$SONA_SPARK_JARS,$param
            is_jars=false
        fi
        command="$command $param"
    fi
done

exec $command 
