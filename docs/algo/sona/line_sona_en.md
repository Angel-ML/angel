
LINE

- Algorithm Description
LINE (Large-scale Information Network Embedding) algorithm, one of the well-known algorithms in the field of Network Embedding, focuses on the design of two objective functions, which respectively describe the first-order similarity (direct edge) and second-order similarity between nodes ( Similar neighbors). in other words,
  - If there are joints between two nodes, then the two nodes are also close in the embedded vector space
  - If the neighbors of two nodes are similar, then in the embedded vector space, the two nodes are also close

For details of the algorithm, please refer to the paper [[1]][ref1]

- Algorithm IO parameters
  - input: hdfs path, undirected graph, nodes need to be consecutively numbered starting from 0, separated by white space or comma, for example:
          0	2
          2	1
          3	1
          3	2
          4	1
  - modelPath: hdfs path, the final model save path is modelPath/epoch_checkpoint_x, where x represents the xth round epoch
  - modelCPInterval: save the model every few rounds of epoch
- Algorithm parameters
  - vectorDim: The vector space dimension of the embedding vector and the vector dimension of the context (meaning that the model space occupied by the second-order optimization is twice the first-order optimization under the same parameters)
  - negSample: The algorithm samples negative sampling optimization, indicating the number of negative sampling nodes used by each pair
  - learningRate: The learning rate affects the results of the algorithm. Too high can easily cause the model to run away. If the result is too large, please lower the
  - BatchSize: the size of each mini batch
  - maxEpoch: the number of rounds used by the sample, the sample will be shuffled after each round
  - Order: Optimize the order, 1 or 2

[ref1]: https://arxiv.org/abs/1503.03578