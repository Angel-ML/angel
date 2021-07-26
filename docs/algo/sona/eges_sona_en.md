# EGES

>  The EGES(enhanced graph embedding with side information) is an algorithm that learns embeddings of nodes by utilizing not only the structural information of a graph but also the side information of each node. Specifically, the side information of a node includes several discrete attributes. The EGES algorithm learns the embeddings of the node itself and its discrete attributes and adaptively adjusts their weights, and the final embedding of a node is the weighted average of the embeddings. Compared with the graph embedding algorithms which learn embeddings based on only the structure of a graph, the EGES algorithm exhibits superior performance. In addition, the EGES algorithm is able to solve the "cold start" problem to some extent, where some new generated nodes have no link to any other node already existed, by initializing the embedding of a new generated node as the average of the embeddings of its discrete attributes. For more details about the EGES algorithm, please refer to the article [EGES](https://dl.acm.org/doi/abs/10.1145/3219819.3219869).

##  Introduction to Algorithm Implementation

We have implemented the EGES algorithm on the Spark on Angel framework, which can handle large-scale industrial data. All embeddings of nodes and attributes and their corresponding weights are stored on Angel PSs. At each epoch, each Spark executor first pulls the input embeddings of the positive nodes and their attributes, the corresponding weights, and the output embeddings of the negative nodes according to the data in the mini batch, and then computes the gradients of the embeddings and the weights. Later, each Spark executor pushes the gradients to the PSs, and the corresponding embeddings and weights on PSs are updated. 

## Running example

### Algorithm IO parameters
  - input： HDFS path. Each data row consists of a pair of co-occurrence nodes and the discrete attributes of the source node，which is split by blank or comma. For example, 
        0	1	sideInfo1	sideInfo2
        2	1	sideInfo3	sideInfo4
        3	1	sideInfo5	sideInfo6
        3	2	sideInfo5	sideInfo6
        4	1	sideInfo7	sideInfo8
  - output： HDFS path. The node ID remapping table, the side information ID remapping table and the final weighted average embeddings of nodes are respectively saved in the 3 subdirectories, namely, "./itemMappingTab", "./sideInfoMappingTab" and "./aggregateItemEmbedding", under the path "output".
  - matrixOutput：HDFS path. The embedding matrix of the nodes and the discrete attributes and the weight matrix are respectively saved in the 2 subdirectories, namely, "./embedding" and "./weightsSI", under the path "matrixOutput".
  - saveModelInterval：the number of epochs between 2 model saving operation
  - checkpointInterval：the number of epochs between 2 model checkpoint operation
  
### Algorithm parameters

  - needRemapping： The indicator determines whether the nodes and the discrete attributes are remapped or not. If needed, "needRemapping" is true, otherwise false. Remapping rule: the nodes are remapped to continuous nonnegative integers starting from 0, while the discrete attributed are similarly remapped starting from (N+1), where N is the max remapped ID of the nodes. 
  - weightedSI： The indicator determines whether the weights of embeddings are adaptively adjusted. If needed, "weightedSI" is true, otherwise false. 
  - numWeightsSI： the number of discrete attributes of each node
  - embeddingDim： the number of dimensions of each embedding
  - numNegSamples： the number of negative samples
  - numEpoch： the number of epochs
  - batchSize： the size of a mini batch
  - stepSize： the learning stepSize
  - decayRate： the diminishing parameter for the stepSize
  - dataPartitionNum：The number of input data partitions which is usually set 3~4 times large as the product of the number of spark executors and the number of cores for each executor.
  - psPartitionNum：The number of data partitions on PSs, which is preferably an integral multiple of the number of PSs, so that the number of partitions carried by each PS is equal. In addition, this parameter is usually set 2~3 times large as the product of the number of PSs and the number of cores for each PS.

### Resource parameters

  - Angel PS number and memory: The product of ps.instance and ps.memory is the total configuration memory of ps. In order to ensure that Angel PSs run normally, you need to configure memory about twice the size of the data stored on PSs. For EGES, 
the formula for calculating the momeroy cost is ( (the total number of nodes and discrete attributes * embeddingDim * 2 * 4) + (the number of nodes * （numWeightsSI + 1）* 4) ) (Bytes).
  - Spark resource configuration: The product of num-executors and executor-memory is the total configuration memory of executors, and it is preferred to allocate memory resource twice as much as the memory cost of the input data. If memory resource is limited, allocating memory resource at the same amount of the memory cost is acceptable, but the processing speed might be slower. In such a situation, we can try by increasing the number of data partitions on spark executors!
  
### Submitting scripts
```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/EGES_output
matrixOutput=hdfs://my-hdfs/EGES_matrixOutput

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
  --class com.tencent.angel.spark.examples.cluster.EGESExample \
  ../lib/spark-on-angel-examples-3.1.0.jar \
  input:$input output:$output matrixOutput:$matrixOutput weightedSI:true numWeightsSI:3 embeddingDim:32 numNegSamples:5 epochNum:10 stepSize:0.01 decayRate:0.5 batchSize:1000 dataPartitionNum:12 psPartitionNum:10 needRemapping:false
```