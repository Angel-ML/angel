# Angel-Graph

Now we are in a  complex network world, where  all things are interconnected , for example, the relationship between people , people and things, things and things has become more complex and diversified. In reality, many problems can be expressed as graphs. With the traditional graph mining, graph representation learning and graph neural network and other graph technologies, we can mine rich information from massive relational structure data , and make up for the lack of single point analysis Finally, it can help financial payment, security risk control, recommendation advertising and many other business scenarios.

## Overview

Angel graph absorbs the advantages of angel parameter server, spark and pytorch, which support the traditional graph computing, graph representation learning and graph neural network, and realize a large-scale distributed graph computing framework with high performance, high reliability and ease of use.

Angel graph has the following core capabilities：

- Complex heterogeneous network. The composition of graph in industry is complex and diverse, and the scale of data often has billions of vertices, tens of billions or even hundreds of billions of edges. Angel graph uses spark on angel or Python for distributed training, which can easily support large-scale graph computing.
- End-to-end graph calculation. The big data ecology of industry is mostly spark and Hadoop. Based on the spark on angel architecture, angel graph can seamlessly connect spark, so as to take advantage of Spark's ETL capabilities and support end-to-end graph training.
- Traditional graph mining. It supports traditional graph algorithms with billions of vertices and hundreds of billions of edges, such as PageRank, kcore analysis node importance, louvain fpr community detection, etc. It provides measurement analysis and rich graph features of vertices for application in business models such as machine learning or recommendation.
- Graph Embedding. It supports the graph embedding algorithm of billions of vertices and hundreds of billions of edges, such as line, word2vec, etc
- Graph Neural Network. Support the graph neural network algorithms with billions of vertices and tens of billions of edges, and use the rich attribute information on vertices or edges for deep learning



## System Architecture

As shown in the figure below, the angel graph framework uses spark and pytorch, and the lower layer uses spark worker, and the upper layer is Angel Parameter Server.

![angel-graph](../img/angel_graph.png)


The spark on angel module in Angel graph enables angel's flexible parameter server plug-in to the native spark, which provides high efficiency data storage / update / sharing services for spark, so it is very suitable for distributed graph computing framework. At the same time, spark on angel uses the original spark interface, so that the algorithm development on the framework can take advantage of Spark's capabilities

#### Spark Component 

- Spark Driver：responsible for controlling the overall calculation logic

- Spark Executor：when performing traditional graph calculation and graph representation learning, spark executor stores immutable data structures such as graph adjacency table / edge table , pulls the required node attributes and other data from PS in each iteration, and push the results back to PS after completing the calculation locally and submits them to PS for update. In the training of graph neural network algorithm, Pytorch C++ backend as the actual computing engine runs in the spark executor in a native way

#### Angel Component 

- Angel Master：manage the life cycle of parameter server
- Angel PS(Parameter Server)： store mutable data such as node attributes in the abstract form of vector ( support to store customized element data structure, support load balancing partition ), update node attributes in place and other flexible calculations customized according to specific algorithm through angel's unique PS function
- Angel Agent：work as  agent for spark executor and parameter server

#### Pytorch Component 

- Python Client：use torchscript syntax to write algorithm model, submit it to spark executor for loading, and complete distributed training and prediction of the model through angel PS




## Build In Graph Algorithms

In order to use this graph computing framework easily, we have implemented several common graph algorithms, which have been fully tested in the internal business of Tencent, and ensure the efficiency and correctness. So users can use them quickly without too much adjustment。

| Algorithm                  | Algorithm Type                   | Algorithm Description                                        |
| -------------------------- | -------------------------------- | ------------------------------------------------------------ |
| PageRank                   | Node importance                  | Classical graph algorithm                                    |
| Hindex                     | Node importance                  | Mixed quantitative index, refer to h index                   |
| Kcore                      | Node importance                  | Extract the key substructures in network                     |
| Louvain                    | Community Detection              | Community division by optimizing the modularity index        |
| Closeness                  | Node Centrality                  | Measure the centrality of vertices                           |
| CommonFriends              | Intimacy calculation             | Compute common friends number                                |
| TriangleCountingUndirected | calculate node's triangle number | Calculate node's triangle number                             |
| LPA                        | Label propagation                | A community discovery or propagation algorithm               |
| ConnectedComponents        | Weak connected components        | Mining weak connected components of Graphs                   |
| LINE                       | Graph Embedding                  | Use the first and second order neighbor information when train representation |
| Word2Vec                   | Graph Embedding                  | A classical representation learning algorithm                |
| GraphSage                  | Graph Neural Network             | Representation learning by aggregating the features of node neighbors |
| GCN                        | Graph Neural Network             | Similar to CNN operation and apply the algorithm to non Euclidean space |
| DGI                        | Graph Neural Network             | Apply  DIM to  complex network                               |



## BenchMark Performance

We compare the performance of graphx and angel graph in two real datasets. The first dataset DS1 contains 0.8 billion vertices and 11 billion edges. The second dataset DS2 contains
2 billion vertices and 140 billion edges. Performance comparison on traditional graph algorithms：

![angel-graph-benchmark](F:/Github/img/angel_graph_benchmark.png)

The detailed introduction to Angel graph, please refer to this paper [PSGraph: How Tencent trains extremely large-scale graphs with Spark?](https://conferences.computer.org/icde/2020/pdfs/ICDE2020-5acyuqhpJ6L9P042wmjY1p/290300b549/290300b549.pdf)



