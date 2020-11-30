# PageRank
>The PageRank algorithm is probably the most famous node importance evaluation algorithm. It was originally proposed by Larry Page and used in the ranking of web pages in Google search. For details, please refer to the paper[The PageRank Citation Ranking:Bringing Order to the Web](http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf).

## 1. Algorithm Introduction
We implemented large-scale PageRank calculation based on Spark On Angel, where ps maintains information of all nodes, including receiving and sending messages and rank value vectors. The calculation of the message and rank value is completed on the spark executor side, and the update is completed through the push / update operation of ps.

## 2. Running example

### Algorithm IO parameters

  - input: hdfs path, each line represents an edge, can be weighted: srcId separator dstId separator weight (optional)
  - output: hdfs path, calculation result output path
  - sep: data column separator (space, comma, tab), default is space

### Algorithm parameters

  - psPartitionNum：The number of model partitions is preferably an integer multiple of the number of parameter servers, so that the number of partitions carried by each ps is equal, and the load of each PS is balanced as much as possible. If the amount of data is large, more than 500 is recommended.
  - dataPartitionNum：The number of input data partitions is generally set to 3-4 times the number of spark executors times the number of executor cores.
  - tol：the tolerance allowed at convergence (smaller => more accurate), the default is 0.01
  - resetProp：the random reset probability (alpha), the default is 0.15

### Resource parameters

  - Angel PS number and memory: The product of ps.instance and ps.memory is the total configuration memory of ps. In order to ensure that Angel does not hang, you need to configure memory about twice the size of the model. For PageRank, the calculation formula of the model size is: number of nodes * 3 * 4 Byte, according to which you can estimate the size of ps memory that needs to be configured under Graph input of different sizes
  - Spark的资源配置：The product of num-executors and executor-memory is the total configuration memory of executors, and it is best to store twice the input data. If the memory is tight, 1 times is also acceptable, but relatively slower. For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient. In a situation where resources are really tight, try to increase the number of partitions!
  
### Submitting scripts
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
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.PageRankExample \
  ../lib/spark-on-angel-examples-3.1.0.jar \
  input:$input output:$output tol:0.01 :5 resetProp:0.15
```

### FAQ
  - At about 10 minutes, the task hangs: The most likely reason is that Angel cannot apply for resources! Since PageRank is developed based on Spark On Angel, it actually involves two systems, Spark and Angel, and their application for resources from Yarn is independently conducted. After the Spark task is started, Spark submits the Angel task to Yarn. If the resource cannot be applied for within a given time, a timeout error will be reported and the task will hang! The solution is: 1) Confirm that the resource pool has sufficient resources 2) Add spakr conf: spark.hadoop.angel.am.appstate.timeout.ms = xxx to increase the timeout time, the default value is 600000, which is 10 minutes
  - How to estimate how many Angel resources I need to configure: To ensure that Angel does not hang, you need to configure about twice the size of the model memory. In addition, when possible, the smaller the number of ps, the smaller the amount of data transmission, but the pressure of a single ps will be greater, requiring certain trade-offs.
  - Spark resource allocation: Also mainly consider the memory problem, it is best to save twice the input data. If the memory is tight, 1 times is also acceptable, but relatively slower. For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient. In a situation where resources are really tight, try to increase the number of partitions!

