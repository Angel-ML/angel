# Louvain(FastUnfolding)

The Louvain (FastUnfolding) algorithm is a classic community detection algorithm, optimized by [modularity] (https://en.wikipedia.org/wiki/Modularity_(networks):
$$Q = \frac{1}{2m} \sum_{vw}\left[A_{vw} - \frac{k_vk_w}{2m}\right]\delta(c_v, c_w).$$

## 1. Introduction to the algorithm
Louvain algorithm contains two processes
 - Modular optimization
 - Community folding
We maintain the community id of the node and the weight information corresponding to the community id through two ps vectors. Each worker on the Spark side maintains a part of the node and the corresponding adjacency information, including the neighbors of the node and the corresponding edge weights.
- In the module optimization phase, each worker calculates the new community attribution of its own maintenance node based on the degree of module change. The community attribution update is updated to ps in real time in the form of a batch.
- In the community folding phase, we construct a new network based on the current community ownership, where the new network node corresponds to the community of the pre-folding network, and the new edge corresponds to the sum of the weights of the direct nodes of the pre-folding network community. . Before starting the next stage of modularity optimization, we need to correct the community id so that the id of each community is identified as the id of a node in the community. Here we use the smallest identifier for all node ids in the community.

## 2. Running

### Parameters

- input: hdfs path, network data, two long integer id nodes per line (if the weighted network, the third float represents the weight), separated by white space or comma, indicating an edge
- output: hdfs path, the community id of the output node, one data per line, indicating the community id value corresponding to the node, separated by tap
- numFold: number of folds
- numOpt: number of module optimizations per round
-eps: module degree increment limit
- batchSize: the size of the node update batch
- partitionNum: Enter the number of data partitions
- isWeighted: Is it entitled?