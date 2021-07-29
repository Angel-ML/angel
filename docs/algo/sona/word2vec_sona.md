# Word2Vec

> Word2Vec算法是NLP领域著名的算法之一，可以从文本数据中学习到单词的向量表示，并作为其他NLP算法的输入。

## 1. 算法介绍

我们使用Spark On Angel实现了基于负采样优化的SkipGram模型，能够处理高达10亿 * 1000维的超大模型。U、V矩阵存储在Angel的PS上，spark executor根据batch数据拉取对应节点以及负采样节点做梯度计算以及更新计算，最后将需要更新的结果push回PS。

## 2. 运行

### 算法IO参数

  - input: hdfs路径，随机游走出来的sentences，word以空白符或者逗号分隔，以数字id为例（但输入数据可以为非数字字符串，后续采用组件自带的重编码功能）比如:

            0 1 3 5 9
            2 1 5 1 7
            3 1 4 2 8
            3 2 5 1 3
            4 1 2 9 4
  - output: 结果保存hdfs路径, 最终的embedding结果保存路径为 output/CP_x, 其中x代表第x轮, 结果保存的格式分隔符可通过配置项指定：
              
            spark.hadoop.angel.line.keyvalue.sep=(可支持space、comma、tab、bar、colon等，默认是colon)
              
            spark.hadoop.angel.line.feature.sep=(可支持space、comma、tab、bar、colon等，默认是colon)
   - saveContextEmbedding: 训练时选择是否保存context embedding，保存该embedding可用于增量训练    
   - extraInputEmbeddingPath: 从外部加载预训练的节点input embedding向量做初始化用做增量训练，数据格式默认为: 节点id：embedding向量(向量用空格分隔，如 123:0.1 0.2 0.1)，分隔符可通过配置项指定
                               
            spark.hadoop.angel.line.keyvalue.sep=(可支持space、comma、tab、bar、colon等，默认是colon)
                               
            spark.hadoop.angel.line.feature.sep=(可支持space、comma、tab、bar、colon等，默认是colon)   
   - extraContextEmbeddingPath: 从外部加载预训练的节点context embedding向量做初始化用做增量训练，数据格式默认为: 节点id：embedding向量(向量用空格分隔，如 123:0.1 0.2 0.1)，分隔符可通过配置项指定
                                
            spark.hadoop.angel.line.keyvalue.sep=(可支持space、comma、tab、bar、colon等，默认是colon)
                                
            spark.hadoop.angel.line.feature.sep=(可支持space、comma、tab、bar、colon等，默认是colon)
   - nodeTypePath: 跑异构skip-gram需要的节点类型路径(如metapath游走的结果), 数据格式为: 节点id 分隔符 类型id   
   - saveModelInterval: 每隔多少个epoch保存一次模型
   - checkpointInterval: 每隔多少个epoch写一次checkpoint

### 算法参数

  - embedding: 嵌入的向量空间维度，即为embedding向量的维度
  - negative: 算法采样的是负采样优化，表示负采样的个数
  - window: 训练的窗口大小，使用前window与后window的词
  - epoch: 总的迭代轮数
  - stepSize: 学习率很影响该算法的结果，太高很容易引起模型跑飞的问题，如果发现结果量级太大，请降低该参数
  - batchSize: 每个mini batch的有多少训练句子，由于实现的时候是去取batch句子去训练，所以不能设的太大，一般设置100以内
  - psPartitionNum: 模型分区个数，最好是parameter server个数的整数倍，让每个ps承载的分区数量相等，让每个PS负载尽量均衡, 数据量大的话推荐500以上
  - dataPartitionNum: 输入数据的partition数，一般设为spark executor个数乘以executor core数的3-4倍
  - remapping: 是否需要对节点进行重新编码，取值true或者false(word2vec目前只能支持节点ID属于一个连续的整数空间(int 范围内)，最好在运行word2vec之前做好ID的映射。如果设置为true，首先会做一次ID的映射并输出一个映射文件)

### 资源配置建议

  - Angel PS个数和内存大小: 为了保证Angel不挂掉，需要配置模型大小两倍左右的内存。Word2Vec模型大小的计算公式为：节点数 * Embedding特征的维度 * 2 * 4 Byte，比如说1kw节点、100维的配置下，模型大小差不多有8G大小，那么配置instances=4, memory=4就差不多了。另外Word2Vec算法的瓶颈主要在通信，因此ps的数量最好能与worker数量对等，最好不要少于1：3，这样ps通信压力不会太大。
  - Spark的资源配置: num-executors与executor-memory的乘积是executors总的配置内存，最好能存下2倍的输入数据。 如果内存紧张，1倍也是可以接受的，但是相对会慢一点。 比如说100亿的边集大概有600G大小， 50G * 20 的配置是足够的。
  
### 任务提交示例
进入angel环境bin目录下
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

### 常见问题
  - 在差不多10min的时候，任务挂掉: 很可能的原因是angel ps申请不到资源！由于Word2Vec基于Spark On Angel开发，实际上涉及到Spark和Angel两个系统，它们的向Yarn申请资源是独立进行的。 在Spark任务拉起之后，由Spark向Yarn提交Angel的任务，如果不能在给定时间内申请到资源，就会报超时错误，任务挂掉！ 解决方案是： 1）确认资源池有足够的资源 2） 添加spakr conf: spark.hadoop.angel.am.appstate.timeout.ms=xxx 调大超时时间，默认值为600000，也就是10分钟
  - 如何估算我需要配置多少Angel资源: 参考资源配置建议章节
