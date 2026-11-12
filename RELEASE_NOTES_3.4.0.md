# Angel 3.4.0 Release Notes

## 1. Overview

Angel 3.4.0 focuses on **expanding graph computing capabilities, broadening deployment options, and hardening stability**. All Spark on Angel (SONA) graph algorithms are now consolidated into `spark-on-angel-mllib`, with dozens of new graph algorithms covering connected components, community detection, graph embedding, ranking, statistical features, link prediction, and ANN retrieval. This release also adds general-purpose ML modules for time series, KD-Tree, frequent pattern mining, and anomaly detection. On the parameter-server side, it introduces STANDALONE / EMBEDDED deployment modes, a metrics framework, and FlatBuffers serialization, along with a large set of reliability fixes for the transport layer, snapshot recovery, and memory management.

Runtime requirements: JDK 1.8, Scala 2.12, Spark 3.3.x (compiled against 3.3.1), and Hadoop 2.x (compatible with 2.9+/2.10). The release has been verified end to end on a Hadoop 2.10.2 + Spark 3.3.4 YARN cluster in yarn-cluster mode with 45 graph, embedding, and ANN algorithm examples. Results of the click-rate and link-prediction algorithms match local mode line by line.

> **Upgrade note**: Graph algorithm packages have moved from `com.tencent.angel.graph.*` to `com.tencent.angel.spark.ml.graph.algo.*`, and the module dependency direction has changed. See "Major Features - 7. Code Organization and Compatibility Changes".

## 2. Major Features

### 2.1 Expanded Graph Algorithms (spark-on-angel-mllib)

| Category | New in this release |
|---|---|
| Connected components | SCC, BatchedCC, CompressCC, CutCC |
| Community detection | LPAX, LouvainCPM, LouvainDensity, LouvainRepair, LouvainOpt, SeedNeighbors (subgraph), Modularity |
| Graph embedding | BGE, LINE v2 / v3, MetaPath2Vec V1/V2, DocVec, EmbeddingPooling, Word2Vec V1, DeepWalk V4, Node2Vec V5 |
| Ranking / centrality | ClusterRank, KCoreDC, KCoreCompress, PPR, WHIndex, MultiAttrHIndex, Intimacy, Cooccurrence / WeightedCooccurrence (biclique), CommonFriendsRecall, TopRelation, MultiAttrAggregate |
| Statistics / structure | Directed triangle counting, directed LCC, GroupDirectedTriangle, WeightedMotifCount V2/V3, SSSP / MSSP, SSHopLayout, BiDirectionalEdges, NonLinkComFriends |
| Click-rate features | OneHop, OneHopCom, OneHopWeighted, TwoHopCom, LBSBased |
| Edge / neighbor features | EdgeAggregate, FunctionalEdgeAggregate, InteractRate, NeighborVectorDistance |
| Link prediction & indexing | LinkPrediction, PostIndex, PostIndex4Hnsw, ReIndexPlus, String2IntReIndex, DeIndex, RePartition |
| Clique mining | KClique0, KCliqueV2 |
| ANN retrieval | IVFHnsw, KMeans-IVFHnsw, and recall evaluation (EvaluateRecall), alongside the existing BruteForce and Hnsw |

### 2.2 General Machine Learning Modules

- **Time series**: ARIMA, AutoARIMA, AR, EWMA, GARCH, Holt-Winters, ACF / PACF, ADF / KPSS / Ljung-Box tests, Box-Cox, differencing, and time-series feature extraction.
- **Tree models**: New KD-Tree (KDTTrainer / FPKDTTrainer / KDTPredictor). GBDT now supports a configurable `storageLevel`, feature sampling via `featSampleRatio`, and dense input format.
- **Anomaly detection & classification**: IsolationForest, LOF, KNN, OutlierTransformer.
- **Frequent pattern mining**: Apriori, FPGrowth, PrefixSpan.
- **Feature engineering**: About 25 preprocessing components (binning, scaling, missing-value imputation, mutual information, correlation, sampling, splitting, and more). Also new: Chinese word segmentation (based on ansj_seg), PMML export (based on jpmml-sparkml), and Angel-backed ALS recommendation.
- **Online learning**: FTRL / FtrlFM / AsyncFTRL now use in-place vector operations to reduce memory usage. FTRL can be initialized from a `ModelContext`.

### 2.3 PS Deployment Modes

- **STANDALONE mode**: Runs without YARN or Spark. The AM runs inside the submitting process, and PS / Worker run as independent JVMs that can be launched on multiple hosts via ssh. It ships with the `angel-cluster-up`, `angel-ps-launch`, and `angel-standalone-submit` scripts and is configured through `angel.standalone.*`.
- **EMBEDDED mode**: The AM runs embedded in the caller process, for example the Spark driver.
- **Attach mode**: Spark jobs can attach to an already running PS cluster via `spark.angel.ps.master` or `spark.hadoop.angel.master.host/port` (`AngelAttachClient`). Readiness APIs such as `waitPS` / `waitAllPSReady` are provided.

### 2.4 Observability

- New `com.tencent.angel.metrics` framework (`AngelMetricsReporter`, `AngelPSMetrics`, `AngelPSLoad`, etc.), plus a PS metrics service, transport-server metrics, and PS container lifecycle reporting. Configured via `angel.metrics.*`.

### 2.5 Graph Computing Foundations (angel-ps-graph)

- Neighbor storage abstraction `Neighbor` / `IntNeighbor` with LONGARRAY / INTARRAY storage and RANDOM / ALIASTABLE / SIMPLEREJECT sampling methods.
- Node features support a combined dense + sparse (field) + sequence layout. `GraphNode` supports train / validation edges, edge labels, and edge timestamps.
- New PSFs: time-aware neighbor sampling, edge-feature sampling, node / edge label read and write, edge timestamp initialization, Node2Vec V2 batched path pulling, and more.
- Dynamic neighbor tables now use batch-compressed `CompressedDynamicLongArray` to reduce memory. Half-precision (FP16) serialization is supported.
- New FlatBuffers serialization (flatbuffers-java 23.5.26), used for embedding model output.

### 2.6 Transport Layer and Stability Enhancements

- **Client**: write-idle heartbeats, a retry queue ordered by next retry time, a user request deadline (`angel.psagent.user.request.deadline.ms`, default 30 minutes), and bounded request thread pools that fail fast when saturated.
- **Server**: automatic epoll selection, TCP_NODELAY, write watermarks, idle connection reclamation (`angel.netty.matrixtransfer.server.channel.idle.seconds`), and port-binding retries.
- **PSF timeout protection**: `-Dangel.psf.timeout.ms` (default 5 minutes). On timeout, the Spark task fails and is retried, so jobs no longer hang indefinitely.
- **JVM tuning**: new `angel.{ps,worker}.jvm.overhead.mb` (default 1024), `angel.ps.jvm.g1.*`, and `angel.{am,ps,worker}.java.opts.append.mode`.

### 2.7 Code Organization and Compatibility Changes

- **Graph algorithm migration**: The algorithm layer of `spark-on-angel-graph` has moved to `spark-on-angel-mllib`. Packages change from `com.tencent.angel.graph.<category>.<algo>` to `com.tencent.angel.spark.ml.graph.algo.<category>.<algo>`. `community` is renamed to `communitydetection`, and `kclique` moves to `cliques/kclique`. `spark-on-angel-graph` now keeps only the low-level PS models and common utilities.
- **Reversed module dependency**: `spark-on-angel-graph` no longer depends on `spark-on-angel-mllib`; instead, `spark-on-angel-mllib` depends on `spark-on-angel-graph`. Depend on `spark-on-angel-mllib` to use graph algorithms.
- **Removed**: Leiden and MotifCounting (replaced by WeightedMotifCount V2/V3). Utility methods such as `iteratorBatch` and `runOnTimer` have been removed from `ml.util.Utils`.
- **Kubernetes restored**: Master/PS/Worker pod submission, scheduling, attempt retries and cleanup are reconnected. The distribution includes the image Dockerfile and entrypoint. Single-node kind/Kubernetes 1.31 validation covers two-PS matrix updates and save/load, a Worker gradient task, PS pod replacement and model recovery, and application resource cleanup. Fixes include legacy Fabric8 WebSocket Origin errors for URLs without explicit ports and mixed Hadoop 2/3 runtime classpaths. Algorithm E2E validation includes native LR training/save/load/two-batch-size prediction and PageRank on real Spark Kubernetes driver/two-executor pods, using both external-PS attach and driver-launched PS. `angel.kubernetes.client.classpath` isolates incompatible Spark/Angel Fabric8 versions; late Worker task configuration and prediction-tail parameter loading are fixed. Multi-node, all algorithms, remote registries and secure Hadoop are not validated.
- **Default changes**: PS defaults to `-Xms=-Xmx` with AlwaysPreTouch enabled. The JVM non-heap overhead rises from 512 MB to 1024 MB. The retry backoff cap is now 5 seconds.
- **Example parameter changes**: Some example parameters are renamed, e.g. COPRA `weightIndex` → `weightCol` and PageRank `dataPartitionNum` → `partitionNum`.
- **Dependency upgrades**: PS and SONA now use the modular Netty 4.1.74 jars instead of `netty-all-4.1.42`. `stream` is upgraded to 2.9.6, and jackson is pinned to 2.13.4 to match Spark 3.3.

### 2.8 Examples and Test Data

- 70 new cluster examples, 74 new local examples, and 65 ML and time-series examples, plus a shared `ClusterExampleUtils` helper.
- angel-ps adds JSON configurations and local examples for MixedLR, NFM, and PNN.
- New test datasets under `data/` (ann, bc, clickrate, eges, embedding_pooling, karate_club_network, docvec, and more), so all new examples run out of the box.

## 3. Bug Fixes

### 3.1 PS Core and Transport

- Fixed PS container allocation failures on Hadoop 2.9+/2.10, where YARN's built-in `ResourceRequest` comparator threw an NPE when comparing an unset execution type (`YarnContainerAllocator`).
- Fixed a resource leak where channels, direct-memory ByteBufs, and seqId mappings were not released when a request was marked FATAL or retries were abandoned (`MatrixTransportClient`).
- Fixed a race between responses and timeouts that could let timeout handling override a successful response.
- Fixed send-failure handling that called `Thread.stop()` with no effect; it now goes through the normal request-failure path.
- Fixed the dispatcher terminating the whole worker on a single exception.
- Fixed PS recovery events being dropped by throttling, which left clients stuck after a PS recovered.
- Fixed `ChannelManager2`'s connection checker thread never being started, and a concurrency race in `getChannel`.
- Fixed `MapResponseCache.canMerge` misjudging duplicate responses.
- Fixed Master / PS ports being taken between the availability check and the bind ("Address already in use"); ports are now reserved and binding is retried.
- Fixed the app timeout check busy-looping and repeatedly dispatching InternalError events.
- Fixed `ioRatio` being hard-coded to 70 in NIO mode, which made the setting ineffective.
- Fixed containers being killed by YARN with exit 137 because of the fixed 512 MB non-heap overhead. The overhead is now configurable, and the AM log explicitly explains exit 137.
- Fixed libjars entries without a scheme not resolving to the Spark staging directory (`AngelYarnClient`).

### 3.2 PS Storage and Snapshots

- Fixed `deepClone` in `ElementElementMapStorage`, `LongElementMapStorage`, and `StringElementMapStorage` iterating over a newly created empty map and returning empty results.
- Fixed an NPE when `dataSize` of map storages raced with concurrent writes / rehashing, which caused heartbeat failures.
- Fixed an NPE in `UniversalEmbeddingNode.deepClone` when an array is null.
- Fixed truncated snapshots being silently loaded as empty matrices. A `CorruptedSnapshotException` is now thrown, and recovery falls back to an older snapshot directory.
- Fixed `PartAsyncOptimParam` serialization undercounting its length by one int field.

### 3.3 Graph Algorithms

- Fixed timing races in several algorithms that read from PS without waiting for the Future returned by `psfUpdate`. Affected algorithms: LinkPrediction (neighbor-table check failure followed by a hang), SLPA, COPRA, KCoreDC, LINE, and LINE v3.
- Fixed OneHopWeighted using the default Hash partitioner when load-balanced partitioning was disabled, which made column-range-based PSFs fail with `node id is not in range`. Also fixed a `ClassCastException` caused by a mismatched parameter type in its read path.
- Fixed ClusterRank's out-degree and neighbor-edge matrices having no partitioner set, so they also fell back to Hash partitioning and went out of range. The block column count is now rounded up and is at least 1.
- Fixed LINE v3 text output producing nothing for String / `IElement` keys, which resulted in 0-byte result files. Output is now consistently UTF-8 encoded.
- Fixed an NPE in ANN `GetSearchResult` when saving a model with empty rows or results.
- Fixed `GetNeighbor` handling of empty neighbors, and failures caused by rows containing nulls when loading edges.

### 3.4 Machine Learning

- Fixed an out-of-bounds error in FPGBDT training caused by sizing the `isFeatUsed` array by the number of used features.

### 3.5 Examples and Deployment

- Fixed `cluster/LINEExample` unconditionally calling `setMaster`, which conflicted with `spark-submit --master yarn` on Spark 3. New cluster examples create the SparkContext through `ClusterExampleUtils.start`.
- Fixed `spark-on-angel-env.sh` referencing `netty-all-4.1.42` and `stream-2.7.0`, which are not shipped in the release package, leaving PS / executors with missing dependencies.
- Fixed the Word2Vec example hard-coding a local native library path, the EgoNetwork example ignoring the `srcIndex` / `dstIndex` / `nodeIndex` parameters, and MetaPath2Vec not filtering empty lines when reading node types.
- Fixed `MetricsServlet` failing to initialize in SONA unit tests because a transitive dependency pulled in jackson 2.7.7 under Spark 3.3.
