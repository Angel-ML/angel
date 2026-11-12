# Angel 系统参数手册

适用版本：Angel 3.4.0；源码基线：`f6ace0a8`；整理日期：2026-11-12。

本手册以当前源码声明、读取点和启动流程为依据，不把旧部署文档或示例配置中的取值当作源码默认值。内容分为“如何配置”和“逐项参数参考”两部分。

## 1. 范围与分类索引

[完整参数参考](angel_system_parameters_reference.md)覆盖 `AngelConf` 中 **384 个不同配置键，来自 385 条声明**，包括非 `angel.` 前缀的控制/RPC 配置。每项包含中文含义、源码默认值、单位/类型、源码位置、消费位置和适用限制。

| 分类 | 参数数 | 主要内容 |
| --- | ---: | --- |
| 任务、输入输出与提交 | 45 | action、作业身份、数据路径、提交配置、ACL、资源分发 |
| Master / AM | 22 | Master 配额、状态、心跳、慢节点检测、I/O |
| Worker、WorkerGroup 与 Task | 28 | Worker 配额、任务迭代、数据块存储、失败容忍 |
| PS、矩阵存储、分区与快照 | 48 | PS 配额、备份、分区、存储、自适应表示 |
| PS / Worker JVM、GC 与内存 | 42 | G1/Parallel GC、直接内存、非堆预算、参数追加 |
| RPC、Netty 与流控 | 68 | 连接池、超时、重试、队列、在途请求、保活 |
| PSAgent、客户端与一致性 | 25 | 客户端代理、时钟、缓存、独立 PSAgent |
| STANDALONE、SSH 与 Attach | 24 | 主机池、远程启动、日志、手动 PS、就绪检查 |
| 日志、指标与慢请求检查 | 30 | 通用 metrics、RPC metrics、ADS 资源 metrics |
| 模型解析、Python、Serving 与扩展 | 26 | 扩展入口及模型服务边界配置 |
| Kubernetes 配置 | 26 | Master/PS/Worker Pod 的镜像、资源、认证、标签与卷；已恢复启动链，真实集群尚待验证 |

本页另外整理 Spark 专用映射、未在 `AngelConf` 中声明的关键键、矩阵属性、脚本环境变量和配置示例。分类只为检索方便，不表示每个参数在所有模式下均生效。

不声称覆盖全部 Hadoop/Spark 参数和每个算法的 `ml.*` 超参数。算法参数仍应参考[算法文档](../algo/angel_or_spark_on_angel.md)、具体 Example 和对应 `MLConf`/Param 类。

## 2. 默认值的四个层次

1. **源码默认常量**：`DEFAULT_*` 只是代码中的后备值；`new AngelConf()` 不会把所有常量自动写进 Configuration。
2. **消费处默认值**：例如 `conf.getInt(key, 60)`；可能没有对应 `DEFAULT_*`，也可能与声明默认不同。
3. **入口映射/派生值**：Spark 转换器、客户端和提交器可改写参数，或生成应用 ID、输出路径。
4. **最终运行值**：应用代码和后续透传还可覆盖；真正执行时应检查各进程得到的 Configuration。

例子：

| 参数/行为 | Angel 源码默认 | Spark on Angel / 入口行为 |
| --- | --- | --- |
| `angel.am.memory.gb` | `4` | `spark.angel.master.memory` 默认 `2g`，映射后通常为 `2` |
| `angel.ps.memory.gb` | `4` | `spark.ps.memory` 默认 `4g` |
| `angel.output.path.deleteonexist` | `false` | `spark.ps.out.overwrite` 默认 `true` |
| `angel.ps.heartbeat.interval.ms` | `5000` ms | Spark 初始部署模式为严格大写 `LOCAL` 时写为 `200` ms |
| `angel.psagent.cache.sync.timeinterval.ms` | `200` ms | Spark 转换器写为 `100000000` ms |
| `angel.tmp.output.path.prefix` | 无配对默认常量 | 下游默认 `/tmp/angel/<user>`，最终含应用 ID 与 UUID |

**单位不能只按常量名猜测。** `.ms` 是毫秒、`.sec/.seconds` 是秒、`.mb/.gb` 是对应容量单位；没有单位的字段应看消费 API。旧内存 MB→GB 转换按 `ceil(MB / 1024)`，不是除以 1000。

## 3. 原生 Angel 的配置入口与优先级

### 3.1 XML 与应用命令行

使用 `ConfUtils.initConf(args)` 的入口，加载顺序为：

1. Hadoop `Configuration` 的默认资源。
2. 设置了 `HADOOP_HOME` 时添加其 `etc/hadoop/yarn-site.xml` 和 `hdfs-site.xml`。
3. 添加 `$ANGEL_HOME/conf/angel-site.xml`；未设置 `ANGEL_HOME` 会失败。
4. 添加 `angel.app.config.file` 指定的用户 XML；该文件路径可来自应用命令行或前面加载的配置。
5. 对应用命令行参数逐项执行 `conf.set`。
6. 合并用户文件/JAR 分发资源，装载用户作业 JAR，展开配置中的环境变量。

XML 按 Hadoop 资源规则合并，包括 `<final>` 等语义；应用命令行 `conf.set` 是显式覆盖。之后的应用/客户端代码仍可能改写值，不存在不分入口的全局优先级表。

以下是**配置文件内容示例**，保存为自己的 XML 文件：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>angel.deploy.mode</name>
    <value>YARN</value>
  </property>
  <property>
    <name>angel.ps.number</name>
    <value>2</value>
  </property>
  <property>
    <name>angel.ps.memory.gb</name>
    <value>4</value>
  </property>
  <property>
    <name>angel.ps.cpu.vcores</name>
    <value>2</value>
  </property>
  <property>
    <name>angel.ps.log.level</name>
    <value>INFO</value>
  </property>
</configuration>
```

通过 `-Dangel.app.config.file=/absolute/path/job.xml` 交给相应应用入口；`angel.ml.conf` 是算法 JSON，不是上述系统参数 XML。

### 3.2 Java API

```java
Configuration conf = new Configuration();
conf.addResource(new Path("/absolute/path/job.xml"));
conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
conf.setInt(AngelConf.ANGEL_PS_NUMBER, 2);
conf.setInt(AngelConf.ANGEL_PS_MEMORY_GB, 4);
AngelClient client = AngelClientFactory.get(conf);
```

上述片段依赖 `org.apache.hadoop.conf.Configuration`、`org.apache.hadoop.fs.Path`、`com.tencent.angel.conf.AngelConf`、`com.tencent.angel.client.AngelClient` 和 `AngelClientFactory`。直接 API 不会因为采用 `Configuration` 就自动执行 `ConfUtils.initConf` 的环境/命令行流程；数据、输出和任务类仍按使用的入口设置。

### 3.3 应用参数与 JVM 参数不要混用

`ConfUtils` 支持：

| 写法 | 含义 |
| --- | --- |
| `-Dangel.ps.number=2` | 主类之后的应用参数，进入 Hadoop Configuration |
| `--angel.ps.number 2` | 同上，键和值各一个 argv |
| `angel.ps.number:2` | 同上，按第一个冒号分割 |
| `jar /absolute/path/job.jar` | 设置并装载用户 JAR |

重复键最后出现者生效。不把 `--key=value`、`-D key=value` 当成已支持形式。

```bash
java -Xmx2g -Djava.io.tmpdir=/work/tmp \
  -cp "$CLASSPATH" com.tencent.angel.utils.AngelPSClusterMain \
  -Dangel.deploy.mode=STANDALONE \
  -Dangel.ps.number=2
```

- 主类**之前**的 `-Djava.io.tmpdir` 是 JVM system property。
- 主类**之后**的 `-Dangel.ps.number` 是 Angel 应用参数。
- 把所有 `angel.*` 放进 `ANGEL_JAVA_OPTS` 不会自动变成 Hadoop 配置。
- `${ENV_NAME}` 展开由 `ConfUtils` 的环境处理逻辑完成；环境变量不是一层通用“最高优先级配置”。

依据：[ConfUtils.java](../../angel-ps/core/src/main/java/com/tencent/angel/utils/ConfUtils.java#L42)、[AngelRunJar.java](../../angel-ps/core/src/main/java/com/tencent/angel/utils/AngelRunJar.java#L42)。

## 4. Spark on Angel 的专用参数

以下默认值来自 `AngelPSContext.convertToHadoop`，不直接复制 `AngelConf.DEFAULT_*`。

| Spark 键 | 默认/单位 | 对应 Angel 配置或含义 |
| --- | --- | --- |
| `spark.app.name` | 该读取点无默认 | `angel.job.name = appName + "-ps"` |
| `spark.master` | 该转换器可缺省 | 缺省或以 local 开头按本地模式处理；真正 Spark 启动仍须有效 master |
| `spark.ps.mode` | `YARN` | `angel.deploy.mode`；local master 且非 EMBEDDED 时初始强制 LOCAL |
| `spark.yarn.queue` | `root.default` | Angel 的 `queue`；不是 `angel.queue` |
| `spark.angel.master.memory` | `2g` | AM 内存，容量解析后取整数 GB |
| `spark.ps.instances` | `1` | PS 总数后备来源；不是 executor 数，也不是自动发现的外部 PS 数 |
| `spark.ps.cores` | `1` | 每 PS CPU vcore 配额 |
| `spark.ps.memory` | `4g` | 每 PS 内存，容量解析后取整数 GB |
| `spark.ps.extraJavaOptions` | 未设置时不覆盖 | PS JVM 额外参数，写入 `angel.ps.java.opts` |
| `spark.ps.jars` | 空串 | PS 依赖资源，写入 `angel.job.libjars`；YARN 应使用可访问资源路径 |
| `spark.ps.log.level` | `INFO` | 同时配置 AM 和 PS 日志级别 |
| `spark.ps.class` | `com.tencent.angel.ps.ParameterServer` | PS 入口类 |
| `spark.ps.out.overwrite` | `true` | 控制 Angel 内部输出目录存在时是否删除；**不控制 `GraphIO.save` 算法结果** |
| `spark.ps.out.tmp.path.prefix` | 不主动设置 | 写入 `angel.tmp.output.path.prefix`，消费处可计算默认路径 |
| `spark.hadoop.fs.defaultFS` | 当前 Spark Hadoop 配置；再缺省 `file://` | 用于 Angel 临时模型和日志路径 |
| `spark.app.id` | 运行时取得 | 写入 `angel.application.id`，用于资源 staging |
| `spark.yarn.stagingDir` | FS 用户主目录 | 资源 staging 基目录 |
| `spark.yarn.dist.files` | 未设置 | 逗号分隔资源列表，转换为 Spark staging 文件路径 |
| `spark.hadoop.angel.ps.number` | Optional | PS 数量映射的首选值；需与实际外部集群一致 |
| `spark.angel.ps.master` | 未设置 | 外部 Master 地址 `host:port`；设置后触发 Attach，不再启动新集群 |
| `spark.hadoop.angel.master.host` | 未设置 | Attach 替代形式的主机地址；缺失才回退 `spark.hadoop.angel.am.host` |
| `spark.hadoop.angel.master.port` | 未设置 | 与前一项配对；这条解析链不把 am.port 当替代项 |
| `spark.hadoop.angel.ps.attach.skip.wait` | Angel 默认 `false` | 仅跳过 Attach 的 `waitPS`，不跳过 `waitAllPSReady` |

### 4.1 Spark 配置转换顺序

1. `AngelSparkHadoopUtil.getConfiguration` 取得 Hadoop 配置，包括 Spark 的 `spark.hadoop.*` 映射。
2. 写入上表专用映射，以及 PS-only 运行模式、action、临时路径和资源值。
3. 写入 Spark staging/user resource 信息。
4. 直接 `angel.*` 配置透传，覆盖同名键。
5. `spark.angel.*` 透传，进一步覆盖同名键。
6. EMBEDDED 分支强制 `angel.ps.request.resource.use.minibatch=false`；local+EMBEDDED 重建配置，仅保留 `angel` 前缀项。

普通无专用映射的键可用 `--conf spark.hadoop.angel.<suffix>=...`。但对被第 2 步主动设置的键，不能假定 `spark.hadoop.*` 始终最高优先级。例如 `spark.hadoop.angel.ps.memory.gb` 可能被 `spark.ps.memory` 的默认映射覆盖；资源项优先采用专用 `spark.ps.*`，避免给同一值配置多套来源。

PS 数量在第 2 步的顺序为：

```text
spark.hadoop.angel.ps.number
  > 初始 Hadoop 配置中的 angel.ps.number
  > spark.ps.instances
  > 读取点默认 1
```

随后仍可能被第 4、5 步覆盖。`angel.ps.total.cores` 在透传前按 PS 数量 × 核数计算，后续覆盖数量/核数不会自动重算它。不要通过晚期覆盖制造互相矛盾的资源配置。

初始 LOCAL 心跳、EMBEDDED 和 Kubernetes 分支依据先前的局部部署变量执行；晚期只覆盖 `angel.deploy.mode` 并不会重新执行全部部署判断。

依据：[AngelPSContext.scala:568–682](../../spark-on-angel/core/src/main/scala/com/tencent/angel/spark/context/AngelPSContext.scala#L568)、[AngelSparkHadoopUtil.scala](../../spark-on-angel/core/src/main/scala/org/apache/spark/AngelSparkHadoopUtil.scala)。

### 4.2 JVM 调整方法的配置，不等于自动生效

`AngelPSContext.adjustExecutorJVM` 存在以下读取项；本次源码检索未找到正常初始化链对它的调用，不能写成始终自动执行。

| 键 | 方法内默认/含义 |
| --- | --- |
| `spark.ps.executor.extraJavaOptions` | Optional；存在即采用，包括空串，跳过自动内存拆分 |
| `spark.executor.extraJavaOptions` | 空串；与 PS 扩展选项拼接 |
| `spark.executor.memory` | 方法内默认 `2048M`，不足时按 2048 MB 计算 |
| `spark.ps.usedirectbuffer` | `true`；改变预留直接内存比例，不直接切换 Netty allocator |

令 `M=max(executorMemoryMB, 2048)`，扣除 512 MB 后，直接内存取剩余量的 30%（true）或 20%（false），堆为剩余部分，年轻代取堆的 30%。默认输入得到 direct=460 MB、heap=1076 MB、young=322 MB。方法生成的 CMS 等旧 JVM 参数有 JDK 兼容性限制，不建议未经核对直接套到较新 JDK。

依据：[AngelPSContext.scala:753–795](../../spark-on-angel/core/src/main/scala/com/tencent/angel/spark/context/AngelPSContext.scala#L753)。

### 4.3 SparkConf 和提交示例

```scala
val conf = new SparkConf()
  .setAppName("angel-parameter-example")
  .setMaster("yarn")
  .set("spark.ps.instances", "2")
  .set("spark.ps.cores", "2")
  .set("spark.ps.memory", "4g")
  .set("spark.ps.jars", psJars)
  .set("spark.hadoop.angel.ps.router.type", "range")
```

`SparkConf` 来自 `org.apache.spark.SparkConf`，`psJars` 需由调用方提供实际 PS 依赖资源列表。range 不是所有算法的通用推荐；它适用于按列区间访问的相应 PSF/CSR 模型。

以下提交使用已配置好的发布包 `bin/spark-on-angel-env.sh`，在发布包 `bin` 目录执行；输入输出和 queue 改为自己的路径：

```bash
source ./spark-on-angel-env.sh

"$SPARK_HOME/bin/spark-submit" \
  --master yarn \
  --deploy-mode cluster \
  --queue default \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 2g \
  --driver-memory 1g \
  --conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
  --conf spark.ps.instances=2 \
  --conf spark.ps.cores=1 \
  --conf spark.ps.memory=2g \
  --conf "spark.ps.jars=$SONA_ANGEL_JARS" \
  --jars "$SONA_SPARK_JARS" \
  --class com.tencent.angel.spark.examples.cluster.PageRankExample \
  "$ANGEL_HOME/lib/spark-on-angel-examples-${ANGEL_VERSION}.jar" \
  mode:yarn input:hdfs:///data/edges output:hdfs:///results/pagerank \
  partitionNum:4 psPartitionNum:4
```

`--num-executors/--executor-memory` 管 Spark；`spark.ps.*` 管 Angel PS，二者独立。PageRank Example 会用应用 `mode:` 覆盖 master，并无默认地读取 `spark.executor.extraJavaOptions`，因此这里显式提供。最后的 `partitionNum`、`psPartitionNum` 是该算法参数，不是通用系统键。

## 5. 部署模式与运行模式

| 部署方式 | 组件位置与配置重点 | 限制 |
| --- | --- | --- |
| LOCAL | Master/PS/Worker 在本 JVM 线程中；用于本地调试 | 全局本地集群上下文，不能把多个独立应用并发启动当受支持能力 |
| YARN | Master、PS、Worker 由 YARN 分配容器 | 需要 Hadoop 集群、共享资源和足够队列容量 |
| STANDALONE | Master 在提交 JVM；PS/Worker 为独立本机或 SSH 子 JVM | 仍需 Hadoop Java 依赖；无需运行 YARN 服务 |
| EMBEDDED | Spark Driver 内 Master，Executor JVM 内 PS | 需要应用显式执行 PS 启停流程，不是只改一个配置 |
| Attach | 连接已启动的外部 Master/PS | 不是部署枚举值；不拥有外部集群生命周期 |
| KUBERNETES | Master、PS、Worker 通过 Fabric8 Kubernetes API 创建 Pod；支持 PS-only/Spark launch | 已恢复启动链，需有效集群、镜像、共享文件系统及网络；尚未实测真实集群 |

`angel.running.mode` 是另一概念：通常 `ANGEL_PS_WORKER` 为原生 PS+Worker，`ANGEL_PS` 为第三方计算引擎使用 PS 服务；枚举还保留 `ANGEL_LOCAL`。不要将 `LOCAL` 写作 `angel.running.mode` 值。

推荐采用大写 `LOCAL/YARN/STANDALONE/EMBEDDED/KUBERNETES`。当前 `DeployUtils` 有大小写敏感比较，不能依据某个上游入口的忽略大小写判断，宣称全链路均不区分大小写。

依据：[AngelDeployMode.java](../../angel-ps/core/src/main/java/com/tencent/angel/AngelDeployMode.java)、[RunningMode.java](../../angel-ps/core/src/main/java/com/tencent/angel/RunningMode.java)、[DeployUtils.java](../../angel-ps/core/src/main/java/com/tencent/angel/master/deploy/DeployUtils.java)、[LocalCluster.java](../../angel-ps/core/src/main/java/com/tencent/angel/localcluster/LocalCluster.java)。

### 5.1 LOCAL 初始化超时

`angel.local.master.startup.timeout.seconds` 默认 **60 秒**，用于等待本地 Master 初始化并连接；初始化失败会传递原始原因，并清理启动失败的本地集群。

它不是总的 PS 集群启动超时。后续 `waitForAllPS` 循环没有自己的总体 deadline；PS 数量不符或一直不就绪仍需外围超时保护。

### 5.2 STANDALONE 配置与脚本环境变量

| 环境变量 | 默认/用途 | 作用范围 |
| --- | --- | --- |
| `JAVA_HOME` | 无自动 JDK 默认；需可用 JDK | 各独立启动脚本 |
| `ANGEL_HOME` | 独立脚本默认脚本父目录；`ConfUtils` 直接入口必须显式设置 | 发布包目录和系统 XML |
| `ANGEL_LIB` | `$ANGEL_HOME/lib` | 本机依赖 JAR 目录 |
| `ANGEL_USER_JAR` | 未设置；冒号分隔 | 追加用户 classpath |
| `ANGEL_JAVA_OPTS` | 未设置 | standalone-submit 中用于 Master/提交 JVM；ps-launch 中用于手动 PS JVM，均在主类之前 |
| `HADOOP_CONF_DIR` | 未设置 | 脚本可将其前置到 classpath；不是 `ConfUtils` 的显式 XML加载入口 |
| `ANGEL_REMOTE_LIB` | cluster-up 后备为本机 lib glob | 远程 classpath，所有 PS 主机须预部署 |
| `ANGEL_SSH_USER` | cluster-up 默认 `$USER` | 转换为 standalone SSH 用户 |
| `ANGEL_SSH_PORT` | `22` | 远程 SSH/SCP 端口 |
| `ANGEL_SSH_OPTS` | 未设置 | 附加 SSH/SCP 参数，不建议关闭主机校验 |
| `ANGEL_REMOTE_JAVA` | `java` | 远程 Java 路径 |
| `ANGEL_REMOTE_WORKDIR` | 未设置时由 Java 端派生 | 远程本次作业的 job.xml 工作目录 |
| `ANGEL_REMOTE_LOG_DIR` | Java 端后备 `/tmp/angel-standalone-logs` | 远程 stdout/stderr |
| `ANGEL_PS_OPTS` | 未设置 | cluster-up 写入 `angel.ps.java.opts` |
| `ANGEL_MASTER_LOCATION_FILE` | 未设置 | Master 地址文件；相应命令行参数可覆盖 |
| `ANGEL_CLUSTER_LOG_FILE` / `ANGEL_LOG_DIR` | 未设置时脚本使用发布包 logs 等后备目录 | cluster-up 日志文件/目录，见脚本实际优先级 |
| `ANGEL_PS_LISTEN_HOST` | 未设置时 PS 探测网络接口 | angel-ps-launch 的 PS 对外地址 |
| `ANGEL_PS_LOG4J` / `ANGEL_PS_LOG_LEVEL` | 未设置 | angel-ps-launch 注入日志 JVM 参数 |
| `ANGEL_VERSION` | 应按实际发布版本配置 | 当前 angel-standalone-submit 仍无条件导出 **2.5.0**，存在覆盖 3.4.0 环境变量的风险 |
| `SPARK_HOME` / `ANGEL_HDFS_HOME` | spark-on-angel-env.sh 中需填写 | Spark 可执行目录/PS 资源的 HDFS 根目录 |
| `SONA_ANGEL_JARS` / `SONA_SPARK_JARS` | 环境脚本生成 | 分别用于 PS libjars 和 Spark --jars |

`angel-cluster-up` 的 `--detach`、`--ps-per-host`、`--ready-timeout`、`--log-file`、`--pid-file`、`--master-location-file`、`--external-ps`、`--ps-manifest`、`--job-conf-dir` 是**脚本参数**，不是可直接放入 Hadoop XML 的同名键。角色日志参数映射为相应 `angel.standalone.*`，请以脚本 `--help` 为准。

脚本注释“强制 STANDALONE”不等于无法覆盖：它先放 `-Dangel.deploy.mode=STANDALONE` 再拼用户参数，用户后来重复同键时最后值生效。版本覆盖和部署参数顺序见[angel-standalone-submit](../../angel-ps/bin/angel-standalone-submit#L41)。

独立 PS-only 启动范式，实际目录和 JDK 路径由调用方设置：

```bash
export JAVA_HOME=/path/to/jdk8
export ANGEL_HOME=/absolute/path/angel-3.4.0-bin
export ANGEL_VERSION=3.4.0
RUN_DIR=$(mktemp -d /tmp/angel-parameters.XXXXXX)
CP=$(find "$ANGEL_HOME/lib" -maxdepth 1 -name '*.jar' \
  ! -name 'hadoop-client-api-*.jar' ! -name 'hadoop-client-runtime-*.jar' | paste -sd:)

"$JAVA_HOME/bin/java" -Xmx2g -cp "$CP" \
  com.tencent.angel.utils.AngelPSClusterMain \
  -Dangel.deploy.mode=STANDALONE \
  -Dangel.am.host=127.0.0.1 \
  -Dangel.ps.number=2 \
  -Dangel.ps.java.opts=-Xmx2g \
  -Dfs.defaultFS=file:/// \
  "-Dangel.save.model.path=file://$RUN_DIR/model" \
  "-Dangel.master.location.file=$RUN_DIR/master.address"
```

这使用 Java 入口避开当前脚本版本覆盖风险；classpath 排除当前发布包中与 Hadoop 2 重叠的 Hadoop 3 client 聚合 JAR。多机不能公布 `127.0.0.1`；部署依赖必须一致且所有节点可访问。Master 实际 RPC 端口来自公布的地址，不保证就是 `angel.am.port`。

普通原生训练/预测仍需配置任务类和输入输出路径；只有 PS-only 集群主类会补齐 dummy splitter、运行模式和缺省输出。具体操作见[独立部署指南](standalone_deploy.md)。

### 5.3 Attach

```text
--conf spark.angel.ps.master=<实际Master地址>
--conf spark.hadoop.angel.ps.number=<外部集群真实PS总数>
```

- 显式地址触发 Attach；不需要再设 `spark.ps.mode=STANDALONE`。
- Driver 和 Executor 必须访问 Master 及全部 PS 公布的地址。
- `waitAllPSReady` 要求 PS 位置数**严格等于**配置数量且都非 NOTREADY，100 ms 轮询，没有循环自身的总超时。
- `angel.ps.attach.skip.wait=true` 只影响 `waitPS`；不跳过 `waitAllPSReady`，只能在外部已确认就绪时使用。
- `PSContext.stop` 清理本地代理及其跟踪的矩阵，不停止外部集群；不能据此假设模型跨作业自动永久保留。
- `--files`/`spark.yarn.dist.files` 表示资源分发，不会自动把文件内容读成系统 XML；Attach 也不负责给已有外部 PS 重新部署资源。

依据：[AngelPSContext.scala:307–458](../../spark-on-angel/core/src/main/scala/com/tencent/angel/spark/context/AngelPSContext.scala#L307)、[AngelClient.java:1125](../../angel-ps/core/src/main/java/com/tencent/angel/client/AngelClient.java#L1125)、[AngelAttachClient.java](../../angel-ps/core/src/main/java/com/tencent/angel/client/attach/AngelAttachClient.java)。

### 5.4 EMBEDDED

设置 `spark.ps.mode=EMBEDDED` 只是选择模式。应用需在创建模型前调用 `AngelPSStartUtils.startPS(sc, N)`，在 SparkContext 停止前调用 `stopPS()`，让 barrier stage 真正在 Executor JVM 内启停 PS。

- `spark.executor.instances` 在 helper 的 EMBEDDED 分支没有默认，必须显式配置。
- 每个 Executor JVM 为一个嵌入 PS；建议固定 N 个 Executor、每个 1 core、启动分区 N，关闭动态扩缩容，保证启动 stage 覆盖每个 JVM。
- `local[N]` 只是同 JVM 多线程；本地嵌入验证使用 `executor.instances=1` 与 `startPS(sc, 1)`。
- PS 和 Spark 共享堆与进程，`spark.ps.memory` 不是独立 PS JVM 的实际内存上限。
- helper 不自动校验所有数量配置一致；显式 `spark.hadoop.angel.ps.number` 仍可能覆盖 helper 写入的 instances。

依据：[AngelPSStartUtils.scala](../../spark-on-angel/core/src/main/scala/com/tencent/angel/spark/util/AngelPSStartUtils.scala)。

### 5.5 Kubernetes（恢复支持）

恢复复用了原有 Kubernetes Pod feature steps、ConfigMap 和 watcher/poller。历史独立 Master 在上游 `122019a8` 清理中已删除；本次用薄入口 `AngelKubernetesApplicationMaster` 复用当前 Master 和状态机，而不是覆盖 LOCAL/STANDALONE 的修复。

- `angel.deploy.mode=KUBERNETES`：原生提交通过 `AngelKubernetesClient` 创建 Master Pod；Master 的 Kubernetes allocator 负责 PS/Worker Pod。
- `angel.kubernetes.master`：Kubernetes API server URL；`angel.kubernetes.namespace` 默认 `default`；`angel.kubernetes.serviceaccount` 默认 `default`，应指定拥有本 namespace 所需 Pod/ConfigMap 操作权限的账号。
- `angel.kubernetes.container.image` 必须为已构建、各节点可拉取的 Angel 3.4.0 镜像。发布目录新增 `kubernetes/Dockerfile` 与 `kubernetes/entrypoint.sh`；可在发布包根目录用 `docker build -f kubernetes/Dockerfile -t <registry>/angel:3.4.0 .` 构建。本地运行镜像已构建并加载至 kind 节点；远程 registry 推送与镜像拉取仍未验证。
- Master 绑定 Pod IP 与 `angel.kubernetes.master.port`（默认 `9078`），必须与客户端及 PS/Worker env 一致；不要仅设置随机 `angel.listen.port.range` 期待客户端发现新端口。
- `ANGEL_PS_WORKER` 使用一组一个 Worker 的原有调度模型；`ANGEL_PS` 只部署 PS。资源配额来自 `angel.am.*`、`angel.ps.*`、`angel.worker.*`，不是 Spark executor 配额。
- properties ConfigMap 挂载到 `/opt/resource/conf/angel-site.properties`，PS/Worker 身份通过 `ANGEL_EXECUTOR_ID`、`ANGEL_EXECUTOR_ATTEMPT_ID` 及 Master 地址环境变量传入。运行镜像必须含用户任务/PSF 依赖；不能把提交机 `file://` 输入路径当成 Pod 可访问的共享数据。
- Kubernetes API 认证子键采用 `angel.kubernetes.authenticate.submission.<suffix>`（提交端）与 `angel.kubernetes.authenticate.<suffix>`（Pod 端）。支持 oauthToken/oauthTokenFile、caCertFile、clientKeyFile、clientCertFile；证书和令牌路径必须在对应运行端存在，不得关闭 TLS/身份校验来绕过部署问题。
- Spark-on-Angel 启动 PS 采用 `spark.ps.mode=KUBERNETES` 和相关 `spark.hadoop.angel.kubernetes.*` 配置；当前转换器还从 Spark `k8s://...` master 提取 API server URL。此部署不需要 `AngelPSStartUtils` 的 EMBEDDED barrier 启动流程。
- Spark 3.3.4 自带 Fabric8 5.12.2，而 Angel 原生 Pod 构造代码使用 Fabric8 3.0.0，不能仅把两套库放进同一个 classpath。Driver 直启 PS 时使用 `spark.hadoop.angel.kubernetes.client.classpath=/opt/angel-native/lib/*`，指向镜像内完整 Angel 原生依赖；桥接以 XML 字符串传递配置并隔离 Hadoop/Scala/Fabric8 类型。未配置时保持原生直接提交行为。Attach 外部 PS 不需要此隔离参数。
- Spark 镜像必须含完整兼容的 Spark distribution 和 Driver/Executor entrypoint，不只是 Spark JAR；本轮使用 Spark 3.3.4 / Scala 2.12.15 / JDK8。提交账号需 namespace 内 Pod/ConfigMap/Service 权限，不应使用 cluster-admin 代替最小授权。应用示例的 `mode:` 若覆盖 `spark.master`，需同时传完整 `mode:k8s://https://kubernetes.default.svc:443`。
- 取消/清理按本应用、角色和业务 attempt 删除 Pod，避免删除其他作业；不能手工移除 Angel 保留标签。现有 Hadoop Kerberos/token 分发能力仍需在安全集群单独验证。

**已实测**：在独立单节点 kind/Kubernetes 1.31 集群，使用 namespace 内 ServiceAccount、证书/token 校验、本地加载镜像与本轮专用共享卷。通过真实 `AngelKubernetesClient` 创建 Master 和两个 PS，跨分区 push/pull 与 save/load 后数值一致；PS+Worker 模式执行六轮梯度任务并验证 loss 下降与最终参数；受控删除 PS Pod 后，Master 创建新 attempt，客户端重新加载模型后数值一致。应用 Pod/ConfigMap 已自动清理。

验证资料位于 `.codebuddy/validation/k8s-e2e-20261112/`。集群 `angel-e2e-20261112` 和独立 `kubeconfig` 保留供复测，不改默认 kubeconfig。API URL 省略 HTTPS 端口已规范为 443，避免 Fabric8 3.0 的无效 WebSocket Origin；镜像启动只在存在 Hadoop 2 common JAR 时排除 Hadoop 3 聚合 JAR。

**算法端到端实测**：原生 Angel `LogisticRegression + GraphRunner` 在两个 PS、一个 Worker 上完成 Agaricus 6513 条训练数据的 5 个 epoch，保存 127 维权重和 bias；新应用加载模型预测 1611 条测试数据。分别使用 batch=128 与 batch=4096 的预测文件完全一致，逐条 margin 与落盘模型独立计算最大误差小于 `5e-7`，测试 accuracy 为 `0.908752`、logloss 为 `0.317438`。

Spark-on-Angel 使用真实 Spark 3.3.4 Kubernetes Driver 与两个 Executor Pod：edge-cut PageRank 通过 Attach 外部 Kubernetes Master/两个 PS；vertex-cut PageRank 通过 Driver 直启独立 Kubernetes Master/两个 PS。两者 34 节点/78 边输出与独立有向 PageRank 参考最大误差分别为 `4.40e-7`、`5.61e-7`。不是 `local[*]` 运行，也不以 submit watcher 退出码单独判定成功；Driver 状态、PS Pod 生命周期和输出正确性均核对。资料位于 `.codebuddy/validation/k8s-algorithms-20261112/`。

本轮还修复 Worker 调度配置冻结导致 `runTask` 晚设置任务类不生效、原生预测末批未拉取 SimpleInputLayer 参数以及预测异常被吞掉。原生模型训练/预测和 Spark 两种部署路径已验证，但**未覆盖所有算法**。多节点网络与卷、远程镜像仓库、Kerberos/delegation token、重载资源与生产规模训练仍待验证。

依据：[Kubernetes Master 入口](../../angel-ps/core/src/main/java/com/tencent/angel/master/AngelKubernetesApplicationMaster.java)、[Kubernetes allocator](../../angel-ps/core/src/main/java/com/tencent/angel/master/deploy/kubernetes/KubernetesContainerAllocator.java)、[Pod 调度器](../../angel-ps/core/src/main/java/com/tencent/angel/kubernetesmanager/scheduler/AngelExecutorPodsAllocator.scala)。

## 6. AngelConf 之外的关键读取项

| 完整键 | 默认/单位 | 配置方法与注意 |
| --- | --- | --- |
| `angel.local.master.startup.timeout.seconds` | `60` 秒 | Hadoop Configuration 或 `spark.hadoop.angel.local.master.startup.timeout.seconds`；只控制 Master 阶段 |
| `angel.am.http.port` | STANDALONE 构造后备 `8888` | Hadoop Configuration；不等同于 Master RPC 端口 |
| `angel.master.host` / `angel.master.port` | 无有效默认，端口后备 `-1` | 独立 AttachClient 接受的地址对；无效时回退完整 am.host/am.port 对 |
| `angel.master.location.file` | 未设置 | PS 集群入口公布实际地址的文件路径 |
| `angel.master.ip` / `angel.master.port` / `angel.conf.keys` | 内部传递值 | Spark TaskContext local properties；不能与外部 master.host 概念混用 |
| `angel.ps.total.cores` | Spark 初始化为有效 PS 数 × cores；其他读取点后备 `2` | 内部资源合计；数量变化后需核对一致性 |
| `angel.ps.index` | 运行时分配 | Embedded PS 内部索引，不建议用户手工指定 |
| `angel.ps.child.opts` | 新 PS java.opts 缺失时采用旧别名 | YARN 旧 JVM 参数入口；优先新键 |
| `angel.psf.timeout.ms` | `300000` ms | **JVM system property**，由 `Long.getLong` 读取；不是一般 Hadoop/SparkConf 参数 |

Spark 传递 JVM-only 的 PSF 超时应采用：

```text
--conf spark.driver.extraJavaOptions=-Dangel.psf.timeout.ms=300000
--conf spark.executor.extraJavaOptions=-Dangel.psf.timeout.ms=300000
```

若已有其他 JVM 参数，应合并而非覆盖丢失。这个超时是同步 PSF 等待层面的值，不与 `angel.matrixtransfer.request.timeout.ms` 或聚合用户请求 deadline 等价。

依据：[AngelLocalClient.java](../../angel-ps/core/src/main/java/com/tencent/angel/client/local/AngelLocalClient.java#L140)、[AngelStandaloneClient.java](../../angel-ps/core/src/main/java/com/tencent/angel/client/standalone/AngelStandaloneClient.java#L120)、[PSMatrixImpl.scala](../../spark-on-angel/core/src/main/scala/com/tencent/angel/spark/models/impl/PSMatrixImpl.scala#L200)、[PSVectorImpl.scala](../../spark-on-angel/core/src/main/scala/com/tencent/angel/spark/models/impl/PSVectorImpl.scala#L76)。

## 7. 每矩阵属性，不是同名全局部署配置

下表来自 `MatrixConf`。它们属于 `MatrixContext` 的 attributes，不能只把名字放入系统 XML 就假设所有矩阵都会采用。

| 属性键 | 源码默认 | 含义/方法 |
| --- | --- | --- |
| `server.partition.class` | `RowBasedPartition.class` | 分区实现类别；具体矩阵通过相关 setter/attribute 指定 |
| `server.partition.storage.class` | `DenseServerRowsStorage.class` | 分区存储类别 |
| `matrix.oplog.type` | `DENSE_DOUBLE` | 操作日志存储类型 |
| `matrix.oplog.enablefilter` | 字符串 `false` | 操作日志零值过滤 |
| `matrix.oplog.filter.threshold` | 字符串 `0.0` | 过滤阈值 |
| `matrix.staleness` | 无配对默认 | 每矩阵一致性/时钟落后值 |
| `matrix.average` | 字符串 `false` | 是否按任务数平均操作日志 |
| `matrix.hogwild` | 字符串 `false` | Hogwild 相关模式，需核对具体执行器 |
| `matrix.load.path` | 空串 | 该矩阵加载路径 |
| `matrix.save.path` | 空串 | 该矩阵保存路径 |
| `matrix.value.type.classname` | 无配对默认 | AnyKey/AnyValue 矩阵元素实现类 |
| `ps.matrix.partition.classname` | `RowBasedPartition` 完整类名 | PS 分区实现类属性 |

此外行数、列数、RowType、indexStart/indexEnd、分区数、key hasher 等是 `MatrixContext` 属性/API，不应杜撰成统一 `angel.*` 配置键。按列区间实现的 CSR/旧邻居 PSF 要显式 RangePartitioner；默认 hash 的桶编号不能被当成节点 ID 范围。

依据：[MatrixConf.java](../../angel-ps/core/src/main/java/com/tencent/angel/conf/MatrixConf.java)、[MatrixContext.java](../../angel-ps/core/src/main/java/com/tencent/angel/ml/matrix/MatrixContext.java#L571)。

## 8. 运维配置建议与已知陷阱

### 8.1 内存与 GC

- YARN 的 `angel.ps.memory.gb` 是容器配额，不是直接 `-Xmx`；框架还预算 direct memory 和 `angel.ps.jvm.overhead.mb`（当前默认 1024 MB）。Worker 对应预算同理。
- PS/Worker 默认使用 G1，并包含固定堆、预触碰与并行引用处理选项；只能在相应 JVM 参数生成路径有效。
- `*.java.opts.append.mode=false` 时，用户提供 java.opts 会整体替换生成参数，可能失去 GC 日志和直接内存上限；采用追加模式也需避免冲突 GC 算法或重复参数。
- STANDALONE 子 JVM 直接使用 PS/Worker java.opts，不保证采用 YARN 的自动堆计算。手工进程必须给足实际堆和容器/主机预算。

### 8.2 超时、队列与网络

- 请求响应超时：`angel.matrixtransfer.request.timeout.ms` 默认 120000 ms。
- 随机退避窗口：`angel.matrixtransfer.retry.backoff.window.ms` 默认 5000 ms。
- 聚合请求 deadline：`angel.psagent.user.request.deadline.ms` 默认 1800000 ms；检查周期默认 30000 ms。
- Master RPC 包装超时：`angel.psagent.master.rpc.timeout.ms` 默认 5000 ms。
- client writer idle 保活默认 30 秒，server all idle 默认 120 秒；保活通常应先于中间设备断连和服务端空闲关闭。
- Netty 写缓冲水位是可写状态/流控信号，不是系统总内存的硬上限。队列容量和并发限制仍需与大 PSF 返回量联动。
- 不把关闭 heartbeat、deadline 或故障保护当通用“性能优化”。先确认正常耗时、内存和依赖问题。

### 8.3 日志与监控

`angel.metrics.enable`、`angel.matrixtransfer.server.metrics.enable`、`angel.metrics.ads.enable` 相互独立。通用默认 reporter 是 Dummy；ADS 默认关闭且 reporter 为空；传输 RPC 指标默认开启。不能只设一个 metrics 开关就认为已接入所有监控。

### 8.4 当前源码必须按真实拼写配置

| 情况 | 说明 |
| --- | --- |
| `action.type`、`queue` | 没有 angel 前缀，不存在自动 `angel.action.type` / `angel.queue` 别名 |
| `angel.output.path`、`angel.tmp.output.path` | 常量名包含 JOB，但实际键没有 job 段 |
| `angel.ps.router.type` | 与 loadbalance-enable 常量误共用同键，路由字符串默认 hash、另一声明布尔默认 true；不要混用 |
| `angel.angel.embedding.feature.sep` | 实际有两次 angel 前缀，不能凭常识改写 |
| `angel.app.serilize.state.file`、`angel.am.slow.check.polices` | 源码历史拼写保留 |
| `angel.job.acl-modify-job` | 源码默认值误为同名键字符串，应按真实授权需求显式设置，而不是扩大权限 |
| `ANGEL_VERSION` | 当前独立脚本硬编码 2.5.0 与发布版本 3.4.0 不一致，需要人工核对 |
| `angel.psagent.fault.inject.stuck.ps.ids` | 仅测试故障注入；生产保持空串 |

### 8.5 确认是否生效

1. 先确认入口是原生 CLI、直接 Java API、Spark launch、Spark Attach 还是独立脚本。
2. 核对实际键、值类型与单位，避免将常量名当配置键。
3. 在参数表查看对应消费者；只声明未直接消费的键不作为调优保证。
4. 核对转换后资源数、路由、输出路径和 JVM 参数；不要只看提交机的配置。
5. 修改一组关联参数后复测代表性任务并观察内存、心跳和队列；不以程序退出码代替算法结果正确性验证。
6. 输出配置时过滤 keytab、token、证书等敏感内容；先确认目录独立再启用 deleteonexist。

## 9. 维护与可追溯性

- 全表每行连接 `AngelConf` 声明位置，并给出当前主代码引用样例；默认表达式保留动态 CPU 计算。
- 无配对默认、没有主代码直接引用、保留 Kubernetes 分支和模型边界配置均显式区分。
- 原始提取、中文释义和引用索引位于 `.codebuddy/validation/parameters-20261112/`；这些是维护资料，不是运行时配置。
- 本次只整理文档，未改变参数实现和默认值，未执行示例提交命令。
- 配置进一步操作见[本地运行](local_run.md)、[YARN 部署](run_on_yarn.md)、[STANDALONE / EMBEDDED / Attach](standalone_deploy.md)。
