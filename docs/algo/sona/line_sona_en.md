# LINE

> LINE (Large-scale Information Network Embedding) algorithm is one of the well-known algorithms in the field of Network Embedding. It embeds graph data into vector space as to use vertor-based machine learning algorithm to handle graph datas.

## Algorithm Introduction

The LINE algorithm is a network representation learning algorithm(also be considered as a preprocessing algorithm for graph data). The algorithm recieve a network as input and, produces the vector representation for each node. The LINE algorithm  mainly focuses on optimizing two objective functions:

![](../../img/line.png)

where, ![](http://latex.codecogs.com/png.latex?O_1) characterizes the first-order similarity between nodes (direct edge), and ![](http://latex.codecogs.com/png.latex?O_2) depicts the second-order similarity between nodes (similar neighbors). in other words,

  - If there are joints between two nodes, then the two nodes are also close in the embedded vector space
  - If the neighbors of two nodes are similar, then in the embedded vector space, the two nodes are also close

For more details, please refer to the paper [[1]](https://arxiv.org/abs/1503.03578)

## Running example

### Algorithm IO parameters

  - input: The edge table hdfs path of the graph, undirected graph, separated by blanks or commas, for example, the edge data without weight is as follows (with weight, enter the weight value of the third column):  
        0	2  
        2	1  
        3	1  
        3	2  
        4	1  
  - output: The result is saved in the hdfs path, and the final embedding result is saved as output/CP_x, where x represents the xth round, and the format separator for saving the result can be specified by the configuration item:
                        
            spark.hadoop.angel.line.keyvalue.sep=(Support space, comma, tab, bar, colon, etc., the default is colon)
                        
            spark.hadoop.angel.line.feature.sep=(Support space, comma, tab, bar, colon, etc., the default is colon)
  - saveContextEmbedding: Choose whether to save the context embedding during the second-order line training, saving the embedding can be used for incremental training         
  - extraInputEmbeddingPath: Load the pre-trained node input embedding vector from the outside for initialization for incremental training. The default data format is: Node id: Embedding vector (vectors are separated by spaces, such as 123:0.1 0.2 0.1), the separator can be set through the configuration item Specify
                                 
            spark.hadoop.angel.line.keyvalue.sep=(Support space, comma, tab, bar, colon, etc., the default is colon)
                                 
            spark.hadoop.angel.line.feature.sep=(Support space, comma, tab, bar, colon, etc., the default is colon)
  - extraContextEmbeddingPath: Load the pre-trained node context embedding vector from the outside for initialization and use for incremental training. Only the second-order line takes effect. The default data format is: Node id: embedding vector (vectors are separated by spaces, such as 123:0.1 0.2 0.1), Separator can be specified by configuration item
                                  
            spark.hadoop.angel.line.keyvalue.sep=(Support space, comma, tab, bar, colon, etc., the default is colon)
                                  
            spark.hadoop.angel.line.feature.sep=(Support space, comma, tab, bar, colon, etc., the default is colon)
  - saveModelInterval: save the model every few rounds of epoch
  - checkpointInterval: write the model checkpoint every few rounds of epoch

### Algorithm parameters

  - embedding: The vector space dimension of the embedding vector and the vector dimension of the context (meaning that the model space occupied by the second-order optimization is twice the first-order optimization under the same parameters)
  - negative: The algorithm samples negative sampling optimization, indicating the number of negative sampling nodes used by each pair
  - stepSize: The learning rate affects the results of the algorithm
  - batchSize: the size of each mini batch
  - epoch: the number of rounds used by the sample, the sample will be shuffled after each round
  - order: Optimize the order, 1 or 2
  - remapping: Remapping the node id or not, true or false
  - psPartitionNum：The number of model partitions is preferably an integer multiple of the number of parameter servers, so that the number of partitions carried by each ps is equal, and the load of each PS is balanced as much as possible. If the amount of data is large, more than 500 is recommended.
  - dataPartitionNum：The number of input data partitions is generally set to 3-4 times the number of spark executors times the number of executor cores
  - sep：Data column separator (space, comma and tab are optional), the default is space
  - isWeight: whether the edge has weight
  
### Resource allocation recommendations

  - Angel PS number and memory: In order to ensure that Angel does not hang up, it is necessary to configure memory that is about twice the size of the model. The calculation formula for the size of the LINE model is: Number of nodes * Embedding feature dimension * order * 4 Byte. For example, in a 1kw node, 100-dimensional, 2-level configuration, the model size is almost 8G in size, then configuration instances=4, memory= 4 is almost there. In addition, the bottleneck of the LINE algorithm is mainly in communication, so the number of ps should be equal to the number of workers, preferably not less than 1:3, so that the pressure of ps communication will not be too great.
  - Spark resource configuration: The product of num-executors and executor-memory is the total configured memory of executors, and it is best to store 2 times the input data. If the memory is tight, 1x is acceptable, but it will be relatively slow. For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient.
  
### Submitting scripts
```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/model

source ./bin/spark-on-angel-env.sh
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
  --class com.tencent.angel.spark.examples.cluster.LINEExample \
  ../lib/spark-on-angel-examples-3.2.0.jar
  input:$input output:$output embedding:128 negative:5 epoch:10 stepSize:0.01 batchSize:1000 numParts:10 remapping:false order:2
```

### FAQ
  - At about 10 minutes, the task hangs: The most likely reason is that Angel cannot apply for resources! Since LINE is developed based on Spark On Angel, it actually involves two systems, Spark and Angel, and their application for resources from Yarn is carried out independently. After the Spark task is started, Spark submits the Angel task to Yarn. If the resource cannot be applied for within a given time, a timeout error will be reported and the task will hang! The solution is: 1) Confirm that the resource pool has sufficient resources 2) Add spakr conf: spark.hadoop.angel.am.appstate.timeout.ms = xxx to increase the timeout time, the default value is 600000, which is 10 minutes
  - How to estimate how many Angel resources I need to configure: Refer to the chapter on resource configuration recommendations.