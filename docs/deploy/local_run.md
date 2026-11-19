# 本地运行模式

当Spark以`local`/`local[N]`模式运行且未显式选择`EMBEDDED`时，Spark on Angel会自动使用Angel的`LOCAL`部署模式：Angel Master和PS直接在Driver进程内启动，不依赖Yarn/HDFS，适合开发调试和功能验证。

### 1. 环境准备

* Java 1.8
* 已编译好的Angel（见[Angel编译指南](source_compile.md)），或已解压的发布包`angel-<version>-bin`

### 2. 在IDE中运行

Angel本地examples位于`spark-on-angel/examples/src/main/scala/com/tencent/angel/spark/examples/local`目录，可直接在IDE(如Intellij IDEA)里面运行相应example的`main`方法。

* 请将运行的工作目录设置为**源码根目录**，examples中使用的测试数据均为相对路径（如`data/bc/edge`）
* 部分examples需要额外参数或数据（如`SSHopLayoutLocalExample`需要`ccInfoPath`），请参考对应源码中的参数说明

### 3. 在命令行中运行

使用发布包`lib`目录下的jar包作为classpath，直接运行local example（需排除与Hadoop 2.x jar冲突的`hadoop-client-api`/`hadoop-client-runtime`）：

```
cd <angel源码根目录>
ANGEL_LIB=dist/target/angel-<version>-bin/lib
CP=$(find "$ANGEL_LIB" -maxdepth 1 -name '*.jar' ! -name 'hadoop-client-api-*.jar' ! -name 'hadoop-client-runtime-*.jar' | paste -sd:)
java -cp "$CP:angel-ps/conf" com.tencent.angel.spark.examples.local.PageRankExample
```

也可以使用`spark-submit`以local模式运行cluster examples。先配置发布包的`bin/spark-on-angel-env.sh`，再进入发布包的`bin`目录执行；无需配置Yarn PS资源或`spark.ps.jars`：

```bash
source ./spark-on-angel-env.sh

${SPARK_HOME}/bin/spark-submit \
    --master 'local[4]' \
    --deploy-mode client \
    --conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
    --conf spark.hadoop.fs.defaultFS=file:/// \
    --jars "$SONA_SPARK_JARS" \
    --class com.tencent.angel.spark.examples.cluster.PageRankExample \
    ./../lib/spark-on-angel-examples-${ANGEL_VERSION}.jar \
    'mode:local[4]' \
    input:file://${ANGEL_HOME}/data/bc/edge \
    output:file:///tmp/pagerank_output \
    partitionNum:4 \
    psPartitionNum:4
```

PageRank示例会用应用参数`mode`覆盖Spark master，因此`--master`和`mode:`必须一致。该示例读取`spark.executor.extraJavaOptions`时没有默认值，需显式提供。使用`file://`路径可避免本地作业误用已有Hadoop配置中的HDFS；输出目录请选用未被其他作业使用的路径。

### 4. 注意事项

* PS的路由方式默认为`hash`；部分算法（如`BiGraphAggre`、`MultiAttrAggregate`、`MultiAttrHIndex`、`TopCandidate`）使用的PSF只支持range分区，需设置`spark.hadoop.angel.ps.router.type=range`（对应local example中已设置）
* 仅设置`spark.ps.mode=EMBEDDED`不足以启动PS，应用还需显式调用嵌入式PS启停流程；本地验证使用单Executor JVM中的一个PS。见[STANDALONE / EMBEDDED / Attach 部署](standalone_deploy.md)
