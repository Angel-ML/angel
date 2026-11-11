# STANDALONE / EMBEDDED / Attach Deployment

Angel 3.4.0 supports the following deployment options. **The Angel PS deployment mode and the Spark master are separate concepts**: a Spark job may run on YARN while attaching to an independently launched PS cluster.

| Option | Master and PS location | Lifecycle |
| --- | --- | --- |
| LOCAL | Master and PS inside the Spark driver process | Managed by the local Spark job; see [Local Running](local_run_en.md) |
| YARN | Angel Master and PS scheduled by YARN | Normally launched and stopped by the Spark driver; see [YARN Deployment](run_on_yarn_en.md) |
| STANDALONE | Master in the submitting JVM; PS in separate local or remote JVMs | Managed by the cluster launcher; no running YARN service required |
| EMBEDDED | Master embedded in the driver; PS embedded in Spark executors | The application must explicitly start and stop PS instances on executors |
| Attach | Spark connects to an existing Angel Master/PS cluster | Spark cleans up its own clients and matrices, not the external cluster |

KUBERNETES deployment is temporarily unavailable in this release and should not be used as a runnable deployment option.

## 1. Environment and Dependencies

* Java 1.8 and a compiled or unpacked `angel-3.4.0-bin` distribution; see [Compilation Guide](source_compile_en.md).
* Set `JAVA_HOME`, `ANGEL_HOME` and `ANGEL_VERSION=3.4.0`. Use an absolute distribution path for `ANGEL_HOME`.
* STANDALONE needs no running Spark, YARN or HDFS service, but still requires the Hadoop Java dependencies in the distribution. Configure `HADOOP_CONF_DIR` and access permissions when using HDFS.
* The current distribution contains both Hadoop 2.x dependencies and Hadoop 3.x `hadoop-client-api` / `hadoop-client-runtime` jars. Using all of `lib/*` may cause class conflicts. The Java command below excludes these two jars. Script deployments and remote PS processes must use a similarly checked runtime dependency directory.
* All PS hosts need matching Angel, Scala and algorithm PSF dependencies and writable working directories. The driver and executors must reach the addresses and RPC ports advertised by the Master and every PS. Do not use `127.0.0.1` across machines.

## 2. Starting a Local Independent PS Cluster

The following starts one Master and one independent PS JVM for same-machine validation. Heap sizes are example settings, not minimum resource guarantees. Set the environment and run this in terminal one:

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

This main class selects the `ANGEL_PS` running mode, starts no workers, and requires no custom `AppSubmitter`. The Master stays in the foreground. Child PS processes inherit the runtime classpath and generated `job.xml`.

Startup prints `ANGEL_MASTER=<host:port>` and writes the actual RPC address to `master.address`. The RPC port is selected from `angel.listen.port.range`, whose default is `20000,30000`; do not treat the default `angel.am.port` as the actual RPC port.

Publishing an address only makes the Master discoverable; it does not prove that every PS is ready. Check PS registration after connecting. Applications may call `AngelPSContext.waitAllPSReady()` after initializing `PSContext`.

## 3. Attaching Spark to External PS

In terminal two, set the same `JAVA_HOME` and `ANGEL_HOME`, and set `RUN_DIR` to the actual directory printed in terminal one. Configure the distribution's `bin/spark-on-angel-env.sh` and run the following from its `bin` directory:

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

This same-machine command has been verified with Java 8 and Spark 3.3.4. For distributed Spark submissions, select the actual Spark master, deployment mode and shared data paths. PageRank's application argument `mode:` must match the Spark master.

### Important Settings and Behavior

* `spark.angel.ps.master=host:port` selects attach instead of launching a new cluster from the driver. Alternatively use the pair `spark.hadoop.angel.master.host` and `spark.hadoop.angel.master.port`. Setting `spark.ps.mode=STANDALONE` is not required.
* Explicitly set `spark.hadoop.angel.ps.number=N` to the **actual number of PS instances in the external cluster**. It takes precedence over `spark.ps.instances`; the latter is not an auto-discovered value. An incorrect count may leave readiness checks waiting indefinitely.
* `--jars` must still distribute Spark client and algorithm dependencies. External PS instances must have the required algorithm PSF jars. `spark.ps.jars` does not replace dependency distribution to Spark executors.
* `AngelPSContext.waitAllPSReady()` waits for exactly N usable PS locations from the Master. The polling loop has no timeout; configure an outer job timeout and inspect Master/PS logs.
* `spark.hadoop.angel.ps.attach.skip.wait=true` affects only `waitPS()`, not `waitAllPSReady()`. Use it only after independently confirming PS readiness.
* `PSContext.stop()` cleans up this application's PSAgent and tracked matrices. The attach client does not request external cluster shutdown. Do not assume models are automatically retained between jobs.

## 4. Cluster Scripts

The distribution provides `angel-cluster-up`, `angel-standalone-submit` and `angel-ps-launch`; the first two also have Windows `.bat` versions. The following describes the Linux Bash scripts. `angel-cluster-up` and `angel-ps-launch` support `--help` for checking arguments.

* `angel-cluster-up` starts a PS-only service by default. The first address is the Master host running the script; remaining addresses are PS hosts. Normal mode requires at least two entries; `127.0.0.1,127.0.0.1` works for a same-machine layout.
* `angel-standalone-submit` defaults to `AngelRunJar` for a custom `AppSubmitter`. Invoking it alone is not equivalent to starting a PS-only service.
* The current `angel-standalone-submit` still hard-codes `ANGEL_VERSION=2.5.0`. Check configurations referencing `${ANGEL_VERSION}` before using this script. The Java entry point above avoids this script's version override.

After preparing conflict-free `runtime-lib` directories on every host and passwordless SSH, a multi-host example is:

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

This starts two PS instances on each of two PS hosts. Attach with `spark.hadoop.angel.ps.number=4`. HDFS configuration, remote Java paths and dependency directories must exist. Use separate output and discovery directories for each cluster launch.

For manually managed PS processes, start the Master with `angel-cluster-up --external-ps`, explicitly pass `-Dangel.ps.number=N`, and specify `--ps-manifest` and `--job-conf-dir`. Put the manifest and `job.xml` on shared filesystem paths accessible to every PS. On the appropriate hosts, run `angel-ps-launch --manifest <path> --ps-id <id>`. PS IDs range from `0` to `N-1`; start only one instance for each ID.

## 5. EMBEDDED Mode

**Setting `spark.ps.mode=EMBEDDED` alone is not enough. The existing PageRank example does not implement the complete embedded PS startup/shutdown chain; the commands above cannot be switched to EMBEDDED by changing a configuration value.**

Applications must integrate `com.tencent.angel.spark.util.AngelPSStartUtils`:

1. Create `SparkContext` and call `AngelPSStartUtils.startPS(sc, N)` before creating models or running the algorithm.
2. This starts PS instances inside executor JVMs through a barrier stage and waits for all PS registrations.
3. Use `finally` to ensure `AngelPSStartUtils.stopPS()` is called before `SparkContext.stop()`. Stopping Spark first prevents scheduling the PS shutdown barrier stage.

For cluster deployment, use a fixed N executors with one core per executor, and set `spark.ps.mode=EMBEDDED`, `spark.executor.instances=N`, `spark.task.cpus=1` and `spark.dynamicAllocation.enabled=false`. Use N startup partitions so the startup stage covers all executors rather than merely providing N task slots. Do not let other `angel.ps.number` settings disagree with N.

Each executor JVM has a single embedded PS instance. `local[N]` still uses one JVM. For local validation, set `spark.executor.instances=1` and call `startPS(sc, 1)`; local thread counts are not PS counts.

PS and Spark tasks share the executor heap and process. `spark.ps.memory` is therefore not an actual independent PS JVM memory limit. This mode needs application integration and scheduling validation; do not assume executor restarts reliably recover all PS state automatically.

## 6. Readiness, Shutdown and Troubleshooting

* Address published but application waiting for PS: check the PS count, registration logs, dependency versions and advertised network addresses. A successful `angel-cluster-up --detach` return does not mean all PS instances have registered.
* Class conflicts or `NoSuchMethodError`: check for mixed Hadoop 2.x / Hadoop 3.x client jars and Scala 2.11 / 2.12 dependencies.
* PageRank unexpectedly selects YARN: check for a missing `mode:local[4]` application argument. Missing `spark.executor.extraJavaOptions` also fails before SparkContext creation.
* Stop a foreground independent cluster with Ctrl-C. For a background cluster, confirm its PID and send SIGTERM with `kill "$(cat <pid-file>)"`. The launcher owns Master/PS shutdown; ending a Spark attach job does not stop the external cluster.
* A detached startup timeout returns a nonzero status but may leave the JVM running. Inspect logs and the PID before waiting longer or shutting it down; do not immediately start a duplicate cluster.
* Clean up temporary run directories only after confirming the processes have exited. Prefer graceful shutdown over `kill -9`.
