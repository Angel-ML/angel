# Node2Vec

>  The Node2Vec algorithm is a well-known graph embedding learning algorithm. It combines the advantages of depth-first search and breadth-first search to sample walking sequences for nodes, which meanwhile extracts both the homophily equivalence and the structural equivalence from graphs. The node embeddings are learned by the Word2Vec algorithm based on the sampled walking sequences. For more details about the algorithm, please refer to the article [Node2vec](https://dl.acm.org/doi/pdf/10.1145/2939672.2939754). In terms of implementation of the Node2Vec, we divide the algorithm to the walking sequences sampling step and the embedding learning step. The class Node2VecExample only focuses on the first step, and the second step should be accomplished by the class Word2VecExample.

##  Introduction to Algorithm Implementation

We have implemented the Node2Vec algorithm on the Spark on Angel framework, which can handle large-scale industrial data. The neighbor set table(without edge weight) or the Alias table(with edge weight) are stored on Angel PSs. At each batch, the Spark executors pull data from the PSs according to the data at this batch and perform node sampling to finally obtain the walking sequences for each node.

## Running example

### IO parameters

  - input： HDFS path，adjacency table with or without edge weights，separated by a given delimiter, for example, 
        0	1	0.3
        2	1	0.5
        3	1	0.1
        3	2	0.7
        4	1	0.3
	where the elements in these 3 columns from the left to the rights are represent the source node ID, the tail node ID and the edge weight.
  - output： HDFS path, walking sequences for all nodes
  
### Algorithm parameters

  - isWeighted： The indicator depends on whether the input adjacency table is weighted or not. This indicator is set true for weighted adjacency table, otherwise it is set false.
  - delimiter： the delimiter which can be space, comma or tab
  - needReplicaEdge： The indicator determines whether to generate replicated edges or not. If the adjacency table contain the edge a->b, while the edge b->a is not in it, this indicator is set true to generate the latter edge, otherwise it is set false.
  - epochNum： the number of walking sequences for each node
  - walkLength： the length of each walking sequence for each node
  - useTrunc： The indicator determines whether to randomly truncate the neighbor set before sampling. 
  - truncLength： the maximum number of nodes contained in the truncated neighbor set
  - batchSize： The size for each mini batch, which is usually set as a value between 500 and 2000
  - psPartitionNum：The number of data partitions on PSs, which is preferably an integral multiple of the number of PSs, so that the number of partitions carried by each PS is equal. In addition, this parameter is usually set 2~3 times large as the product of the number of PSs and the number of cores for each PS.
  - dataPartitionNum：The number of input data partitions which is usually set 3~4 times large as the product of the number of spark executors and the number of cores for each executor.
  - setCheckPoint： The indicator determines whether to use checkpoint for the data stored on the PSs or not, which is set true when using checkpoint and false otherwise.
  - pValue： the parameter for controlling the degree of BFS
  - qValue： the parameter for controlling the degree of DFS

### Resource parameters

  - Angel PS number and memory: The product of ps.instance and ps.memory is the total configuration memory of PSs. In order to ensure that Angel PSs run normally, you need to configure memory about twice the size of the data stored on the PSs. For weighted and unweighted Node2Vec, the formulas for calculating the momeroy cost of the stored data are: the number of nodes * (the average number of neighbors for each node + 1) * 8 (Bytes); the number of nodes * ((the average number of neighbors for each node + 1) * 8 + (the average number of neighbors for each node * 2 * 4)) (Bytes).
  - Spark resource configuration: The product of num-executors and executor-memory is the total configuration memory of executors, and it is preferred to allocate memory resource twice as much as the memory cost of the input data. If memory resource is limited, allocating memory resource at the same amount of the memory cost is acceptable, but the processing speed might be slower. In such a situation, we can try by increasing the number of data partitions on spark executors!
  
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
  --class com.tencent.angel.spark.examples.cluster.Node2VecExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output isWeighted:false delimiter:space needReplicaEdge:true epochNum:1 walkLength:20 useTrunc:false truncLength:6000 batchSize:1000 setCheckPoint:true pValue:0.8 qValue:1.2
```