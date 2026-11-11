# STANDALONE / EMBEDDED / Attach 部署

Angel 3.4.0 支持以下部署方式。**Angel PS部署模式和Spark master是两个独立概念**，例如Spark任务可在Yarn上运行，同时attach到独立启动的PS集群。

| 方式 | Master与PS的位置 | 生命周期 |
| --- | --- | --- |
| LOCAL | Master与PS在Spark Driver进程中 | 由本地Spark作业管理，见[本地运行](local_run.md) |
| YARN | Angel Master与PS由Yarn调度 | 默认由Spark Driver启动和关闭，见[Yarn部署](run_on_yarn.md) |
| STANDALONE | Master在提交JVM中，PS为独立的本机或远程JVM | 由集群启动者管理，不需要运行Yarn服务 |
| EMBEDDED | Master嵌入Driver，PS嵌入Spark Executor | 应用必须显式执行Executor上的PS启停流程 |
| Attach | Spark连接已运行的Angel Master/PS | Spark只清理自己的客户端与矩阵，不关闭外部集群 |

本版本的KUBERNETES部署暂不可用，不应作为可运行的部署选项。

## 1. 环境与依赖

* Java 1.8，已编译或解压的`angel-3.4.0-bin`发布目录，见[编译指南](source_compile.md)。
* 设置`JAVA_HOME`、`ANGEL_HOME`、`ANGEL_VERSION=3.4.0`。`ANGEL_HOME`应为发布目录的绝对路径。
* STANDALONE不需要运行Spark、Yarn或HDFS服务，但仍需要发布包中的Hadoop Java依赖。使用HDFS时需配置`HADOOP_CONF_DIR`和访问权限。
* 当前发布包同时包含Hadoop 2.x依赖与Hadoop 3.x的`hadoop-client-api`、`hadoop-client-runtime`。直接使用整个`lib/*`可能引发类冲突；下面的Java启动命令排除这两个jar。脚本部署和远程PS也需使用经过同样检查的运行时依赖目录。
* 多机部署需要所有PS拥有一致版本的Angel、Scala和算法PSF依赖，并能读写各自的工作目录。Driver与Executor必须能访问Master及全部PS公布的地址和RPC端口。跨机器不能使用`127.0.0.1`。

## 2. 单机启动独立PS集群

以下命令启动1个Master和1个独立PS JVM，用于同机验证；堆大小是示例配置，不是最低资源保证。先设置环境变量，再在终端一执行：

```bash
export JAVA_HOME=/path/to/jdk8
export ANGEL_HOME=/absolute/path/to/angel-3.4.0-bin
export ANGEL_VERSION=3.4.0

RUN_DIR=$(mktemp -d /tmp/angel-standalone.XXXXXX)
printf 'RUN_DIR=%s\n' "$RUN_DIR"
CP=$(find "$ANGEL_HOME/lib" -maxdepth 1 -name '*.jar' ! -name 'hadoop-client-api-*.jar' ! -name 'hadoop-client-runtime-*.jar' | paste -sd:)

"$JAVA_HOME/bin/java" -Xmx1g -cp "$CP" \
    com.tencent.angel.utils.AngelPSClusterMain \
    -Dangel.deploy.mode=STANDALONE \
    -Dangel.am.host=127.0.0.1 \
    -Dangel.ps.number=1 \
    -Dfs.defaultFS=file:/// \
    -Dangel.ps.java.opts=-Xmx1g \
    "-Dangel.save.model.path=file://$RUN_DIR/model" \
    "-Dangel.master.location.file=$RUN_DIR/master.address"
```

该主类自动选择`ANGEL_PS`运行模式，不启动Worker，也无需自定义`AppSubmitter`。Master保持前台运行；PS子进程继承运行时classpath和生成的`job.xml`。

启动后会打印`ANGEL_MASTER=<host:port>`，并把实际RPC地址写入`master.address`。RPC端口从`angel.listen.port.range`选择，默认范围为`20000,30000`，不要把`angel.am.port`的默认值当作实际RPC端口。

地址文件发布只表示Master地址可发现，不等同于全部PS已就绪。连接后需检查PS注册状态；应用可在初始化`PSContext`后调用`AngelPSContext.waitAllPSReady()`。

## 3. Spark attach到外部PS

在终端二设置相同的`JAVA_HOME`、`ANGEL_HOME`，把`RUN_DIR`设为终端一打印的实际目录。配置发布包`bin/spark-on-angel-env.sh`并进入其`bin`目录执行：

```bash
source ./spark-on-angel-env.sh
MASTER=$(cat "$RUN_DIR/master.address")

"$SPARK_HOME/bin/spark-submit" \
    --master 'local[4]' \
    --deploy-mode client \
    --conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
    --conf spark.hadoop.fs.defaultFS=file:/// \
    --conf "spark.angel.ps.master=$MASTER" \
    --conf spark.hadoop.angel.ps.number=1 \
    --jars "$SONA_SPARK_JARS" \
    --class com.tencent.angel.spark.examples.cluster.PageRankExample \
    "$ANGEL_HOME/lib/spark-on-angel-examples-${ANGEL_VERSION}.jar" \
    'mode:local[4]' \
    "input:file://$ANGEL_HOME/data/bc/edge" \
    "output:file://$RUN_DIR/pagerank" \
    partitionNum:4 \
    psPartitionNum:4
```

该单机命令已在Java 8、Spark 3.3.4上验证。多机Spark提交时需按实际集群设置Spark master、部署模式和共享数据路径；PageRank的应用参数`mode:`必须与Spark master一致。

### 关键配置与行为

* `spark.angel.ps.master=host:port`触发attach分支，不再由Driver启动新集群。也可使用`spark.hadoop.angel.master.host`和`spark.hadoop.angel.master.port`参数对；无须额外设置`spark.ps.mode=STANDALONE`。
* 显式设置`spark.hadoop.angel.ps.number=N`为**外部集群实际PS总数**。它优先于`spark.ps.instances`；后者不是自动发现结果。数量配置错误可能导致就绪检查一直等待。
* `--jars`仍需分发Spark客户端和算法依赖；外部PS也必须具有算法所需的PSF jar。`spark.ps.jars`不能代替Spark Executor上的依赖分发。
* `AngelPSContext.waitAllPSReady()`等待Master返回恰好N个可用PS位置，轮询本身没有超时。应在外围设置作业超时并核对Master/PS日志。
* `spark.hadoop.angel.ps.attach.skip.wait=true`只影响`waitPS()`，不会跳过`waitAllPSReady()`。仅在外部已确认PS就绪时使用。
* `PSContext.stop()`会清理当前应用的PSAgent及其跟踪的矩阵，attach客户端不发送关闭外部集群的请求。不能据此假定模型会自动跨作业保留。

## 4. 使用集群脚本

发布包提供`angel-cluster-up`、`angel-standalone-submit`和`angel-ps-launch`；前两者另有Windows `.bat`版本。以下说明对应Linux Bash脚本，`angel-cluster-up`和`angel-ps-launch`支持`--help`检查参数。

* `angel-cluster-up`默认启动PS-only服务。地址列表第一项为运行脚本的Master主机，其余项为PS主机。普通模式至少两项，单机可使用`127.0.0.1,127.0.0.1`。
* `angel-standalone-submit`默认运行`AngelRunJar`，用于自定义`AppSubmitter`；单独调用它并不等同于启动PS-only服务。
* 当前`angel-standalone-submit`仍硬编码`ANGEL_VERSION=2.5.0`。使用脚本前需核对引用`${ANGEL_VERSION}`的配置；上面的Java入口避免了该脚本的版本覆盖。

在所有节点准备好无冲突的`runtime-lib`依赖目录及免密SSH后，多机示例为：

```bash
export ANGEL_LIB=/opt/angel/runtime-lib
export ANGEL_REMOTE_LIB='/opt/angel/runtime-lib/*'
export ANGEL_REMOTE_JAVA=/path/to/jdk8/bin/java

"$ANGEL_HOME/bin/angel-cluster-up" \
    --detach \
    --ps-per-host 2 \
    --master-location-file /tmp/angel-cluster/master.address \
    --pid-file /tmp/angel-cluster/master.pid \
    10.0.0.1,10.0.0.2,10.0.0.3 \
    -Dangel.save.model.path=hdfs:///tmp/angel-cluster/model
```

本例在两台PS主机上各启动2个PS，attach时设置`spark.hadoop.angel.ps.number=4`。需确保HDFS配置、远程Java路径和依赖目录实际存在；每次集群启动使用独立的输出与发现目录。

手动管理PS时使用`angel-cluster-up --external-ps`启动Master，显式传入`-Dangel.ps.number=N`，并指定`--ps-manifest`与`--job-conf-dir`。将manifest及`job.xml`放在各PS可访问的共享文件系统路径，再在相应主机执行`angel-ps-launch --manifest <path> --ps-id <id>`。PS编号范围为`0`到`N-1`，每个编号只启动一个实例。

## 5. EMBEDDED模式

**仅设置`spark.ps.mode=EMBEDDED`不够。现有PageRank示例未实现完整嵌入式PS启停链，不能直接套用上述提交命令切换到EMBEDDED。**

应用需要集成`com.tencent.angel.spark.util.AngelPSStartUtils`：

1. 创建`SparkContext`，在创建模型或执行算法前调用`AngelPSStartUtils.startPS(sc, N)`。
2. 该方法通过barrier stage在Executor JVM内启动PS，并等待所有PS注册。
3. 用`finally`保证调用`AngelPSStartUtils.stopPS()`；必须在`SparkContext.stop()`之前停止PS，否则无法调度停止PS的barrier stage。

集群部署建议固定N个Executor、每个Executor 1 core，并设置`spark.ps.mode=EMBEDDED`、`spark.executor.instances=N`、`spark.task.cpus=1`和`spark.dynamicAllocation.enabled=false`。启动分区数也为N，以确保启动stage覆盖全部Executor，而不是仅具备N个任务槽位。不要让其他配置中的`angel.ps.number`与N不一致。

每个Executor JVM只有一个嵌入式PS实例。`local[N]`仍是一个JVM，本地验证应设置`spark.executor.instances=1`，并调用`startPS(sc, 1)`，不能将本地线程数视为PS数。

PS与Spark任务共享Executor堆与进程，因此`spark.ps.memory`不是独立PS JVM的实际内存上限。该模式需要应用级集成与调度验证，不应假定Executor重启会自动可靠恢复全部PS状态。

## 6. 就绪、关闭与排错

* 地址已发布但应用等待PS：核对PS数量、PS注册日志、依赖版本及公布的网络地址。`angel-cluster-up --detach`返回成功不代表全部PS已注册。
* 类冲突或`NoSuchMethodError`：检查运行时是否混用了Hadoop 2.x与Hadoop 3.x client jar，以及Scala 2.11/2.12依赖。
* PageRank误用Yarn：检查是否缺少应用参数`mode:local[4]`；缺少`spark.executor.extraJavaOptions`也会在创建SparkContext前失败。
* 正常关闭前台独立集群用Ctrl-C；后台集群确认PID后用`kill "$(cat <pid-file>)"`发送SIGTERM，由启动者关闭Master/PS。Spark attach作业退出不会关闭外部集群。
* `--detach`等待超时会返回非零状态，但JVM可能仍在运行，应检查日志及PID后决定继续等待或关闭，不要直接重复启动。
* 测试完成并确认进程退出后，再清理本次运行产生的临时目录。优先正常关闭，避免使用`kill -9`。
