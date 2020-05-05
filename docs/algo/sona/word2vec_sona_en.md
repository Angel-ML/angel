# Word2Vec

>  The Word2Vec algorithm is one of the well-known algorithms in the NLP field. It can learn the vector representation of words from text data and serve as input to other NLP algorithms.

##  Algorithm Introduction

We used Spark On Angel to implement the SkipGram model based on negative sampling optimization, which can handle very large models up to 1 billion * 1000 dimensions. The U and V matrices are stored on Angel PS. Spark executor pulls the corresponding nodes and negative sampling nodes according to the batch data to perform gradient calculation and update calculation. Finally, the results that need to be updated are pushed back to PS.

## Running example

### Algorithm IO parameters
  - input: hdfs path, random walks out of the sentences, word need to be consecutively numbered from 0, separated by white space or comma, such as:
  
          0 1 3 5 9
          2 1 5 1 7
          3 1 4 2 8
          3 2 5 1 3
          4 1 2 9 4
  - output：hdfs path, the final model saving path is modelPath / CP_x, where x represents the xth round of epoch
  - saveModelInterval：save the model every few rounds of epoch
  - checkpointInterval：write the model checkpoint every few rounds of epoch
  
### Algorithm parameters

  - embedding： The word embedding vector dimension
  - negative：The algorithm samples the optimization of negative sampling, indicating the number of negative samples
  - window：The size of the training window, using the words of front window / 2 and back window / 2
  - epoch：Total number of iterations
  - stepSize：The learning rate greatly affects the results of the algorithm. Too high is likely to cause the model to run away. If the result is found to be too large, please lower the parameter
  - batchSize：The size of each mini batch is adjusted according to the length of the sentence, generally within 100
  - psPartitionNum：The number of model partitions is preferably an integer multiple of the number of parameter servers, so that the number of partitions carried by each ps is equal, and the load of each PS is balanced as much as possible. If the amount of data is large, more than 500 is recommended
  - dataPartitionNum：The number of input data partitions is generally set to 3-4 times the number of spark executors times the number of executor cores
  - remapping：Whether the word node needs to be re-encoded, with a value of true or false (currently word2vec can only support continuous numbering of words starting from 0, separated by whitespace or commas, if it is a word or a space that is not consecutively numbered, you need to do remapping, it is recommended to run (The user performs a remapping operation on the data before the algorithm)

### Resource parameters

  - Angel PS number and memory: The product of ps.instance and ps.memory is the total configuration memory of ps. In order to ensure that Angel does not hang, you need to configure memory about twice the size of the model. For word2vec, the calculation formula of the model size is: dictionary size * dimension of Embedding feature * 2 * 4 Byte, for example, 1kw dictionary size, 100-dimensional configuration, the model size is almost 60G, then configure instances = 4 memory = 30 is almost the same. In addition, when possible, the smaller the number of ps, the smaller the amount of data transmission, but the pressure of a single ps will be greater, requiring certain trade-offs
  - Spark resource configuration: The product of num-executors and executor-memory is the total configuration memory of executors, and it is best to store twice the input data. If the memory is tight, 1 times is also acceptable, but relatively slower. For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient. In a situation where resources are really tight, try to increase the number of partitions!
  
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
  --class com.tencent.angel.spark.examples.cluster.Word2vecExample \
  ../lib/spark-on-angel-examples-3.1.0.jar \
  input:$input output:$output embedding:32 negative:5 epoch:10 stepSize:0.01 batchSize:50 psPartitionNum:10 remapping:false window:5
```

### FAQ
  - At about 10 minutes, the task hangs: The most likely reason is that Angel cannot apply for resources! Since Word2Vec is developed based on Spark On Angel, it actually involves two systems, Spark and Angel, and their application for resources from Yarn is carried out independently. After the Spark task is started, Spark submits the Angel task to Yarn. If the resource cannot be applied for within a given time, a timeout error will be reported and the task will hang! The solution is: 1) Confirm that the resource pool has sufficient resources 2) Add spark conf: spark.hadoop.angel.am.appstate.timeout.ms = xxx to increase the timeout time, the default value is 600000, which is 10 minutes