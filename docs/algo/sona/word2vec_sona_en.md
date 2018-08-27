Word2Vec

- Algorithm Description
The original intention of developing the Word2Vec module was to implement another well-known Network Embedding algorithm, the Node2Vec algorithm. The Node2Vec algorithm is divided into two phases: 1) random walk of the network 2) a sequence obtained by running a random walk using the Word2Vec algorithm. This module only handles the second phase of work, but has greater versatility: Currently there is a big difference between the Network Embedding field and the core of the Node2Vec algorithm, usually in the random walk algorithm. For different types of networks and services, it is difficult to find a unified random walk strategy, which is generally closely related to business characteristics, and there are certain difficulties in abstraction, but the super-large-scale Word2Vec algorithm used is generally consistent. Since the graph indicates that the Word2Vec algorithm used for learning needs to be able to handle large lexicons (which can easily reach 1 billion), modules based purely on Spark implementations are difficult to support. The module is based on Spark On Angel and is capable of handling very large models with a height of 1 billion * 1000 dimensions. Word2Vec's theoretical network has a lot of information to refer to, this module implements the SkipGram model based on negative sampling optimization. The very large-scale distributed implementation refers to Yahoo's paper Network-Efficient Distributed Word2vec Training System for Large Vocabularies.

- Algorithm IO parameters
  - input: hdfs path, random walks out of the sentences, word need to be consecutively numbered from 0, separated by white space or comma, such as:
          0 1 3 5 9
          2 1 5 1 7
          3 1 4 2 8
          3 2 5 1 3
          4 1 2 9 4
  - modelPath: hdfs path, the final model save path is hdfs:///.../epoch_checkpoint_x, where x represents the xth round epoch
  - modelCPInterval: save the model every few rounds of epoch
- Algorithm parameters
  - vectorDim: The embedded vector space dimension, which is the vector dimension of the embedding vector and the context - the number of negative samples: The algorithm samples the negative sampling optimization, indicating the number of negative sampling nodes used by each pair.
  - learningRate: The learning rate affects the result of the algorithm. If it is too high, it will easily cause the model to run away. If the result is too large, please lower the parameter.
  - BatchSize: the size of each mini batch
  - maxEpoch: the number of rounds used by the sample, the sample will be shuffled after each round
  - window: the size of the trained window, using the words before window/2 and after window/2