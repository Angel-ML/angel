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

The implementation of the LINE algorithm refers to Yahoo's paper [[2]](https://arxiv.org/abs/1606.08495), which 

1. splitting the Embedding vector into multiple PSs by dimension

2. processing the dot product between nodes partially in each PS,  and merge in spark executors. 

3. calculates the gradient for each node in executors

4. pushes the gradient to all PS, and then update vectors

![line_structure](../../img/line_structure.png)

## Running

### Algorithm IO parameters

- input: hdfs path, undirected graph, nodes need to be consecutively numbered starting from 0, separated by white space or comma, for example:
        0	2
        2	1
        3	1
        3	2
        4	1
- modelPath: hdfs path, the final model save path is modelPath/epoch_checkpoint_x, where x represents the xth round epoch
- modelCPInterval: save the model every few rounds of epoch

### Algorithm parameters

- vectorDim: The vector space dimension of the embedding vector and the vector dimension of the context (meaning that the model space occupied by the second-order optimization is twice the first-order optimization under the same parameters)
- negSample: The algorithm samples negative sampling optimization, indicating the number of negative sampling nodes used by each pair
- learningRate: The learning rate affects the results of the algorithm
- BatchSize: the size of each mini batch
- maxEpoch: the number of rounds used by the sample, the sample will be shuffled after each round
- Order: Optimize the order, 1 or 2