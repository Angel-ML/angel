# HANP

>  HANP(Hop Attenuation & Node Preference) is an algorithm for community detection based on label propagation. For more details about the algorithm, please refer to the article [HANP](https://arxiv.org/pdf/0808.2633.pdf）)

##  Algorithm Introduction

We have implemented the HANP algorithm on the Spark on Angel framework, which can handle large-scale industrial data. The degrees, the labels and the scores of the nodes are stored on the Angel PSs. Each Spark executor pulls the degrees, the labels and the scores of the nodes in its data partitions for computing new labels and scores, which are later pushed to the PSs for updating the information of the corresponding nodes. The pulling, computing and updating steps are iterated until the max iteration number is reached.


## Running example

### Algorithm IO parameters

  - input： HDFS path，adjacency table with or without edge weights，separated by a given delimiter, for example,
        0	1	0.3
        2	1	0.5
        3	1	0.1
        3	2	0.7
        4	1	0.3
  - output： HDFS path, node IDs and their corresponding labels
  
### Algorithm parameters

  - isWeighted： The indicator depends on whether the input adjacency table is weighted or not. This indicator is set true for weighted adjacency table, otherwise it is set false.
  - sep： the delimiter which can be space, comma or tab
  - psPartitionNum： The number of data partitions on PSs, which is preferably an integral multiple of the number of PSs, so that the number of partitions carried by each PS is equal. In addition, this parameter is usually set twice or three times large as the product of the number of PSs and the number of cores for each PS.
  - partitionNum： The number of input data partitions which is usually set three times or four times large as the product of the number of spark executors and the number of cores for each executor.
  - maxIteration： the max iteration number
  - preserveRate： the probability of keeping the information unchanged for each node
  - delta： the attenuation of the score at each propagation
  
### Resource parameters

  - Angel PS number and memory: The product of ps.instance and ps.memory is the total configuration memory of PSs. In order to ensure that Angel PSs run normally, you need to configure memory about twice the size of the data stored on the PSs. For Hanp, the formula for calculating the momeroy cost of the stored data is: (the number of nodes * (4 * 2 + 8 *3)) (Bytes).
  - Spark resource configuration: The product of num-executors and executor-memory is the total configuration memory of executors, and it is preferred to allocate memory resource twice as much as the memory cost of the input data. If memory resource is limited, allocating memory resource at the same amount of the memory cost is acceptable, but the processing speed might be slower. In such a situation, we can try by increasing the number of data partitions on spark executors!

### Submitting scripts
```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/hanp_result

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
  --class com.tencent.angel.spark.examples.cluster.HanpExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output isWeighted:true sep:tab maxIteration:10 preserveRate:0.1 delta:0.1 psPartitionNum:10 partitionNum:12
```