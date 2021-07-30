# Louvain(FastUnfolding)

>Louvain (FastUnfolding) algorithm is a classic community discovery algorithm, which optimizes the [module degree] (https://en.wikipedia.org/wiki/Modularity) indicator to achieve the purpose of community division.

## 1. Introduction to the algorithm
Louvain algorithm contains two processes
 - Modular optimization
 - Community folding
We maintain the community id of the node and the weight information corresponding to the community id through two ps vectors. Each worker on the Spark side maintains a part of the node and the corresponding adjacency information, including the neighbors of the node and the corresponding edge weights.
- In the module optimization phase, each worker calculates the new community attribution of its own maintenance node based on the degree of module change. The community attribution update is updated to ps in real time in the form of a batch.
- In the community folding phase, we construct a new network based on the current community ownership, where the new network node corresponds to the community of the pre-folding network, and the new edge corresponds to the sum of the weights of the direct nodes of the pre-folding network community. . Before starting the next stage of modularity optimization, we need to correct the community id so that the id of each community is identified as the id of a node in the community. Here we use the smallest identifier for all node ids in the community.


## 2. Running

### IO Parameters

- input: hdfs path, input network data, two long integer id nodes per line (if it is a weighted network, the third float represents weight), separated by white space or comma, indicating an edge
- output: hdfs path, where the community corresponding to the output node belongs, one data per line, indicating the community id value corresponding to the node, separated by tap
- sep: input data separator, support: space, comma, tab, default is space
- isWeighted: whether to take the right
- srcIndex: source node index, default is 0
- dstIndex: target node index, default is 1
- weightIndex: weight index, default is 2

### Algorithm Parameters

- numFold: the number of folds
- numOpt: the number of module optimization times per round
- eps: lower limit of modularity increment
- batchSize: the size of the node update batch
- partitionNum: Enter the number of data partitions
- psPartitionNum: ps partition number
- enableCheck: whether to check the community id or degree
- bufferSize: buffer size
- storageLevel: storage level

#### Resource parameters
- Angel PS number and memory size: The product of ps.instance and ps.memory is the total configured memory of ps. In order to ensure that Angel does not hang up, it is necessary to configure memory that is about twice the size of the model. The formula for calculating the size of the Louvain model is: Number of nodes * (8+8+4) Byte, for example, 100 million nodes, the model size is almost 2G in size, then the configuration of instances=2, memory=2 is almost the same. In addition, where possible, the smaller the number of ps, the smaller the amount of data transmission, but the greater the pressure of a single ps, which requires a certain trade-off.
- Spark resource configuration: The product of num-executors and executor-memory is the total configuration memory of executors, and it is best to store 2 times the input data. If the memory is tight, 1x is acceptable, but it will be relatively slow. For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient. When resources are really tight, try to increase the number of partitions!


### Task Submission Example
Enter the bin directory of the angel environment

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/model

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --conf spark.ps.instances=1 \
  --conf spark.ps.cores=1 \
  --conf spark.ps.jars=$SONA_ANGEL_JARS \
  --conf spark.ps.memory=10g \
  --name "kcore angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.LouvainExample \
  ../lib/spark-on-angel-examples-3.1.0.jar
  input:$input output:$output numFold:10 numOpt:3 eps:0.0 batchSize:1000 partitionNum:2 psPartitionNum:2 enableCheck:false bufferSize:1000000 storageLevel:MEMORY_ONLY
```
- At about 10 minutes, the task hangs: The most likely reason is that Angel cannot apply for resources! Since Louvain is developed based on Spark On Angel, it actually involves two systems, Spark and Angel, and their application for resources from Yarn is carried out independently. After the Spark task is started, Spark submits the Angel task to Yarn. If the resource cannot be applied for within a given time, a timeout error will be reported and the task will hang! The solution is: 1) Confirm that the resource pool has sufficient resources 2) Add spark conf: spark.hadoop.angel.am.appstate.timeout.ms = xxx to increase the timeout time, the default value is 600000, which is 10 minutes


