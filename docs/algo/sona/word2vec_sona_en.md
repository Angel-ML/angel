# Word2Vec

>  The Word2Vec algorithm is one of the well-known algorithms in the NLP field. It can learn the vector representation of words from text data and serve as input to other NLP algorithms.

##  Algorithm Introduction

We used Spark On Angel to implement the SkipGram model based on negative sampling optimization, which can handle very large models up to 1 billion * 1000 dimensions. The U and V matrices are stored on Angel PS. Spark executor pulls the corresponding nodes and negative sampling nodes according to the batch data to perform gradient calculation and update calculation. Finally, the results that need to be updated are pushed back to PS.

## Running example

### Algorithm IO parameters
  - input: The hdfs path, the sentences from the random walk, the words are separated by blanks or commas, take the digital id as an example (but the input data can be a non-digital string, and the re-encoding function that comes with the component will be used later) such as:
  
            0 1 3 5 9
            2 1 5 1 7
            3 1 4 2 8
            3 2 5 1 3
            4 1 2 9 4
  - output: The result is saved in the hdfs path, and the final embedding result is saved in output/CP_x, where x represents the xth round, and the format separator for saving the result can be specified by the configuration item:
                          
            spark.hadoop.angel.line.keyvalue.sep=(Support space, comma, tab, bar, colon, etc., the default is colon)
                          
            spark.hadoop.angel.line.feature.sep=(Support space, comma, tab, bar, colon, etc., the default is colon)
  - saveContextEmbedding: Choose whether to save the context embedding during training, saving the embedding can be used for incremental training
  - extraInputEmbeddingPath: Load the pre-trained node input embedding vector from the outside for initialization for incremental training. The default data format is: node id: embedding vector (vectors are separated by spaces, such as 123:0.1 0.2 0.1), the separator can be specified by configuration items
                                                            
            spark.hadoop.angel.line.keyvalue.sep=(Support space, comma, tab, bar, colon, etc., the default is colon)
                                                            
            spark.hadoop.angel.line.feature.sep=(Support space, comma, tab, bar, colon, etc., the default is colon)
  - extraContextEmbeddingPath: Load the pre-trained node context embedding vector from the outside for initialization for incremental training. The default data format is: node id: embedding vector (vectors are separated by spaces, such as 123:0.1 0.2 0.1), the separator can be specified by configuration items
                                                               
            spark.hadoop.angel.line.keyvalue.sep=(Support space, comma, tab, bar, colon, etc., the default is colon)
                                                               
            spark.hadoop.angel.line.feature.sep=(Support space, comma, tab, bar, colon, etc., the default is colon)
  - nodeTypePath: The node type path required to run heterogeneous skip-grams (such as the result of metapath walking), the data format is: NodeId separator TypeId
  - saveModelInterval：save the model every few rounds of epoch
  - checkpointInterval：write the model checkpoint every few rounds of epoch
  
### Algorithm parameters

  - embedding: The word embedding vector dimension
  - negative: The algorithm samples the optimization of negative sampling, indicating the number of negative samples
  - window: The size of the training window, using the words of front window / 2 and back window / 2
  - epoch: Total number of iterations
  - stepSize: The learning rate greatly affects the results of the algorithm. Too high is likely to cause the model to run away. If the result is found to be too large, please lower the parameter
  - batchSize: The size of each mini batch is adjusted according to the length of the sentence, generally within 100
  - psPartitionNum: The number of model partitions is preferably an integer multiple of the number of parameter servers, so that the number of partitions carried by each ps is equal, and the load of each PS is balanced as much as possible. If the amount of data is large, more than 500 is recommended
  - dataPartitionNum: The number of input data partitions is generally set to 3-4 times the number of spark executors times the number of executor cores
  - remapping: Whether the node needs to be re-encoded, the value is true or false (word2vec currently only supports node IDs that belong to a continuous integer space (within the range of int), it is best to do ID mapping before running word2vec. If set to true, (First, ID mapping will be done once and a mapping file will be output)

### Resource allocation recommendations

  - Angel PS number and memory: In order to ensure that Angel does not hang up, it is necessary to configure memory that is about twice the size of the model. The calculation formula for the size of the Word2Vec model is: Number of nodes * Embedding feature dimension * 2 * 4 Byte. For example, in a 1kw node, 100-dimensional, the model size is almost 8G in size, then configuration instances=4, memory= 4 is almost there. In addition, the bottleneck of the Word2Vec algorithm is mainly in communication, so the number of ps should be equal to the number of workers, preferably not less than 1:3, so that the pressure of ps communication will not be too great.
  - Spark resource configuration: The product of num-executors and executor-memory is the total configured memory of executors, and it is best to store 2 times the input data. If the memory is tight, 1x is acceptable, but it will be relatively slow. For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient.
  
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
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output embedding:32 negative:5 epoch:10 stepSize:0.01 batchSize:50 psPartitionNum:10 remapping:false window:5
```

### FAQ
  - At about 10 minutes, the task hangs: The most likely reason is that Angel cannot apply for resources! Since Word2Vec is developed based on Spark On Angel, it actually involves two systems, Spark and Angel, and their application for resources from Yarn is carried out independently. After the Spark task is started, Spark submits the Angel task to Yarn. If the resource cannot be applied for within a given time, a timeout error will be reported and the task will hang! The solution is: 1) Confirm that the resource pool has sufficient resources 2) Add spark conf: spark.hadoop.angel.am.appstate.timeout.ms = xxx to increase the timeout time, the default value is 600000, which is 10 minutes
  - How to estimate how many Angel resources I need to configure: Refer to the chapter on resource configuration recommendations.