# Local Running Mode

When Spark runs with a `local` / `local[N]` master, Spark on Angel automatically uses Angel's `LOCAL` deployment mode unless `EMBEDDED` is explicitly requested. The Angel Master and PS run inside the driver process. No running YARN or HDFS service is required, making this mode suitable for development and functional checks.

### 1. Prerequisites

* Java 1.8
* A compiled Angel checkout (see [Compilation Guide](source_compile_en.md)) or an unpacked `angel-<version>-bin` distribution

### 2. Running in an IDE

Local examples are under `spark-on-angel/examples/src/main/scala/com/tencent/angel/spark/examples/local`. Run an example's `main` method in an IDE such as IntelliJ IDEA.

* Set the working directory to the **repository root**: examples use relative input paths such as `data/bc/edge`.
* Some examples require additional arguments or data; for example, `SSHopLayoutLocalExample` needs `ccInfoPath`. Check the corresponding source before running it.

### 3. Running from the Command Line

Run a local example with the distribution jars on the classpath. Exclude `hadoop-client-api` and `hadoop-client-runtime`, which conflict with the bundled Hadoop 2.x jars:

```bash
cd <repository-root>
ANGEL_LIB=dist/target/angel-<version>-bin/lib
CP=$(find "$ANGEL_LIB" -maxdepth 1 -name '*.jar' ! -name 'hadoop-client-api-*.jar' ! -name 'hadoop-client-runtime-*.jar' | paste -sd:)
java -cp "$CP:angel-ps/conf" com.tencent.angel.spark.examples.local.PageRankExample
```

Cluster examples can also run locally using `spark-submit`. Configure the distribution's `bin/spark-on-angel-env.sh`, then run the following from its `bin` directory. YARN PS resource settings and `spark.ps.jars` are not needed:

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

The PageRank example overrides the Spark master with the application argument `mode`, so `--master` and `mode:` must match. It reads `spark.executor.extraJavaOptions` without a default; provide it explicitly. Using `file://` paths avoids accidentally selecting HDFS from an existing Hadoop configuration. Choose an output path not used by another job.

### 4. Notes

* The default PS router is `hash`. Some algorithms, including `BiGraphAggre`, `MultiAttrAggregate`, `MultiAttrHIndex` and `TopCandidate`, use PSFs that require range partitions. Set `spark.hadoop.angel.ps.router.type=range`; the corresponding local examples already set this option.
* Setting `spark.ps.mode=EMBEDDED` alone is not sufficient. Applications must explicitly start and stop embedded PS instances; local validation uses one PS in the single executor JVM. See [STANDALONE / EMBEDDED / Attach Deployment](standalone_deploy_en.md).
