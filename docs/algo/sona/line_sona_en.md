# LINE

> LINE (Large-scale Information Network Embedding) algorithm is one of the well-known algorithms in the field of Network Embedding. It embeds graph data into vector space as to use vertor-based machine learning algorithm to handle graph datas.

## Algorithm Introduction

The LINE algorithm is a network representation learning algorithm(also be considered as a preprocessing algorithm for graph data). The algorithm recieve a network as input and, produces the vector representation for each node. The LINE algorithm  mainly focuses on optimizing two objective functions:

![line_loss](../../img/line_loss.png)

where, p_1 characterizes the first-order similarity between nodes (direct edge), and p_2 depicts the second-order similarity between nodes (similar neighbors). in other words,

- If there are joints between two nodes, then the two nodes are also close in the embedded vector space
- If the neighbors of two nodes are similar, then in the embedded vector space, the two nodes are also close

For more details, please refer to the paper [[1]](https://arxiv.org/abs/1503.03578)

## Distributed Implemention
There are currently two completely different implementation versions in Spark On Angel: **LINE V1** and **LINE V2**. Each of these two methods has advantages and disadvantages and application scenarios.

In LINE V1, we push the training data to the PS and then training the model in PS. It can avoid a large amount of network IO caused by pulling the model, but it is not suitable for scenes with smaller node embedding dimensions, because it has some limitations: the number of model partitions must be smaller than the node embedding dimension, and the node embedding dimension must be the integer number of partitions. If the node dimension is small, the number of model partitions must be very small, PS is easy to become a bottleneck.

in LINE V2, we pull model from PS to Worker, then training the model in Worker, so it is suitable for the case where the node embedding dimension is not very high. In addition, LINE V2 is much more stable. **In general, we strongly recommend LINE V2**.

### LINE V1

The implementation of the LINE algorithm refers to Yahoo's paper [[2]](https://arxiv.org/abs/1606.08495), which

The model is divided by column, it means that each partition contains all nodes.

1. split the Embedding vector into multiple PSs by dimension

2. process the dot product between nodes partially in each PS,  and merge in spark executors. 

3. calculate the gradient for each node in executors

4. push the gradient to all PS, and then update vectors


### LINE V2

The model is divided by node id range, it means that each partition contains part of node

1. negative sample and pull the nodes embedding from PSs

2. process the dot product between nodes in executors

3. calculate the gradient for each node in executors

4. push the gradient to all PS, and then update nodes embedding

## Running

### Algorithm IO parameters

- input: hdfs path, undirected graph, nodes need to be consecutively numbered starting from 0, separated by white space or comma, for example:
        0	2
        2	1
        3	1
        3	2
        4	1
- output: hdfs path, the final model save path is output/epoch_checkpoint_x, where x represents the xth round epoch
- saveModelInterval: save the model every few rounds of epoch
- checkpointInterval: write the model checkpoint every few rounds of epoch

### Algorithm parameters

- embedding: The vector space dimension of the embedding vector and the vector dimension of the context (meaning that the model space occupied by the second-order optimization is twice the first-order optimization under the same parameters)
- negative: The algorithm samples negative sampling optimization, indicating the number of negative sampling nodes used by each pair
- stepSize: The learning rate affects the results of the algorithm
- batchSize: the size of each mini batch
- epoch: the number of rounds used by the sample, the sample will be shuffled after each round
- order: Optimize the order, 1 or 2
- subSample sub sample or not, true or false
- remapping: remapping the node id or not, true or false

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
  --class org.apache.spark.angel.examples.graph.LINEExample2 \
  ./lib/angelml-0.1.1.jar
  input:$input output:$output embedding:128 negative:5 epoch:100 stepSize:0.01 batchSize:1000 numParts:2 subSample:false remapping:false order:2 interval:5
```