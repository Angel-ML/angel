# K-CORE

> The K-CORE (k-degenerate-graph) algorithm is an important index in complex network research with 
a wide range of applications.

## 1. Algorithm Introduction
We implemented k-core algorithm for large-scale networks based on Spark On Angel.
The ps maintains the node's latest estimation of coreness along with the version number it was last updated for each node.
The Spark side maintains the adjacency list of the network, and pulls the latest coreness estimate in each round.
According to the h-index definition of the network node, the coreness estimate of the node is updated, and then pushed back to the ps in each round.
The algorithm stops until none of corenesses of nodes are updated last round.

## 2. Running

### Parameters

- input： hdfs path of input data, one line for each edge, separate by blank or a comma
- output： hdfs path of output, each line for a pair of node id with its coreness, separated by tap
- partitionNum： the number of partition
- enableReIndex： reindex the node id continuously and start at 0
- switchRate： rate to switch the calculation patten, default to 0.001. 
