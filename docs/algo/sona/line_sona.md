# LINE

>LINE(Large-scale Information Network Embedding)算法，是Network Embedding领域著名的算法之一，将图数据嵌入到向量空间，从达到用针对向量类型数据的机器学习算法来处理图数据的目的

## 1. 算法介绍

LINE算法是一个网络表示学习算法，也可以认为是针对图数据的预处理算法。算法的输入是一个网络拓扑，输出每个节点的向量表示。LINE算法本身在于分别优化两个目标函数：

![](http://latex.codecogs.com/png.latex?\dpi{150}O_1=-\sum_{(i,j)\inE}w_{ij}\logp_1(v_i,v_j))

![](http://latex.codecogs.com/png.latex?\dpi{150}O_2=-\sum_{(i,j)\inE}w_{ij}\logp_2(v_j|v_i))

其中，![](http://latex.codecogs.com/png.latex?p_1)刻画了节点之间的一阶相似性(直接连边)，![](http://latex.codecogs.com/png.latex?p_2)刻画了节点之间的二阶相似性(相似邻居)。换句话说，

  - 如果两个节点之间有连边，那么在嵌入的向量空间中两个节点也要靠近
  - 如果两个节点的邻居是相似的，那么在嵌入的向量空间中，两个节点也要靠近

更详细的细节请参考论文[[1]](https://arxiv.org/abs/1503.03578)



## 2. 分布式实现
LINE 目前有两个独立的实现版本LINE V1和LINE V2， 这两个版本各有优势和适用场景。**一般情况下，建议使用LINE V2版本**。

LINE V1的实现参考了Yahoo的论文[[2]](https://arxiv.org/abs/1606.08495), 将Embedding向量按维度拆分到多个PS上，节点之间的点积运算可以在每个PS内部进行局部运算，之后再拉取到spark端合并。Spark端计算每个节点的梯度，推送到每个PS去更新每个节点对应的向量维度。 它的优势在于避免了传输模型导致的大量网络IO，适用于节点编码向量维度较高的场景，如果节点向量维度较低，它就不合适了。因为LINE V1有如下限制：节点编码维度必须是模型分区个数的整数倍，这限制了模型分区的个数和计算并发度。

![line_structure](../../img/line_structure.png)

LINE V2采用完全不同的实现方式：它使用节点id范围划分模型，没有分区个数限制，同时它需要将模型拉取回executor本地进行计算，因此会产生大量的网络通信开销，不适合节点编码维度高的场景。LINE V2中加入了许多容灾措施，因此更加的稳定。内部业务实测数据表明，在节点编码维度不是很高的情形下（例如128维），LINE V2性能是LINE V1性能的**5倍以上**。


## 3. 运行

### 算法IO参数
  - input： hdfs路径，有向图格式，如果是无向图需要自己把边double一下，节点需要从0开始连续编号。数据格式为文本格式，每一行代表一条边。对于无权图，每一行只有两列，分别代表source节点编号和dest节点编号；如果是边带权重的图，每一行数据有3列，分别是souce节点编号、dest节点编号和边权重。列分隔符可以是空白符、逗号或者tab。
  
  无权图数据格式如下（列分隔符为空格）：
          0	2
          2	1
          3	1
          3	2
          4	1
          
   有权图数据格式如下（列分隔符为空格）：
          0	2 1.5
          2	1 100
          3	1 6.5
          3	2 12.8
          4	1 500
          
  - output： hdfs路径， 最终的模型保存路径为 modelPath/epoch_checkpoint_x,其中x代表第x轮epoch
  - saveModelInterval：每隔多少个epoch保存一次模型
  - checkpointInterval：每隔多少个epoch写一次checkpoint，这个参数主要是容灾用，如果任务运行时间长，建议配置该参数
  
### 算法参数
  - embedding： 嵌入的向量空间维度，即为embedding向量和context的向量维度(意味着同样的参数下，二阶优化占用的模型空间为一阶优化的两倍)
  - negative： 算法采样的是负采样优化，表示每个pair使用的负采样节点数
  - epoch：epoch 个数
  - stepSize： 学习率很影响该算法的结果，太高很容易引起模型跑飞的问题，如果发现结果量级太大，请降低该参数
  - batchSize： 每个mini batch的大小，一般选择1000~10000
  - numParts：模型分区个数，一般是PS个数的4~5倍
  - remapping：是否需要对节点进行重新编码，取值true或者false
  - order 取值1或者2，表示使用一阶相似度还是二阶相似度
  - isWeight 边是否带有权重
  - sep 数据列分隔符，可选comma（逗号）、space（空格）和 tab

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
  --name "line angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class org.apache.spark.angel.examples.graph.LINEExample2 \
  ../lib/spark-on-angel-examples-2.4.0.jar
  input:$input output:$output embedding:128 negative:5 epoch:100 stepSize:0.01 batchSize:1000 numParts:2 subSample:false remapping:false order:2 saveModelInterval:5 checkpointInterval:1 isWeight:false sep:comma
```

### 资源配置和性能数据举例
以下数据均是来自于腾讯内部真实业务

**LINE边不带权版本**

**训练数据集大小**
- 节点数8亿，边数125亿，边不带权的有向图，每个节点Embedding的维度为128

**资源及参数配置**
- 在公共Hadoop集群上测试，该集群非常繁忙，如果是比较空闲的集群，性能数据会远好于下面的数据
- 总消耗资源：内存2.8TB，core总数800
- 100个executor，每个executor 12G内存，4个core
- 100个PS，每个PS 16G内存，4个core

**算法参数配置**
- 负采样数 **negative:5**
- epoch数 **epoch:10**
- 学习率 **stepSize:0.02**
- mini-batch大小（每个mini-batch计算多少条边） **batchSize:5000**
- 模型PS分区个数（模型被划分成多少块放置在PS上，最好是PS个数的整数倍，让每个PS承载的分区数量相等，让每个PS负载尽量均衡） **numParts:500** 
- 节点ID是否需要重新映射（LINE目前只能支持节点ID属于一个连续的整数空间，最好在运行LINE之前做好ID的映射。如果设置为true，首先会做一次ID的映射并输出一个映射文件） **remapping:false**
- 使用的是一阶相似度还是二阶相似度（1或者2） **order:2**
- 每隔多少个epoch保存一次模型 **saveModelInterval:10**
- 每隔多少个epoch做一次checkpoint **checkpointInterval:1**
- 数据列分隔符 **sep:comma**

**性能**
- 每个epoch 45分钟


**LINE边带权版本**

**训练数据集大小**
- 节点数8亿，边数70亿，边带权，每个节点编码成128位

**资源及参数配置**
- 在公共Hadoop集群上测试，该集群非常繁忙，如果是比较空闲的集群，性能数据会远好于下面的数据
- 总消耗资源：内存3.2TB，core总数800
- 100个executor，每个executor 12G内存，4个core
- 100个PS，每个PS 20G内存，4个core

**算法参数配置**
- 负采样数 **negative:5**
- epoch数 **epoch:10**
- 学习率 **stepSize:0.02**
- mini-batch大小（每个mini-batch计算多少条边） **batchSize:5000**
- 模型PS分区个数（模型被划分成多少块放置在PS上，最好是PS个数的整数倍，让每个PS承载的分区数量相等，让每个PS负载尽量均衡） **numParts:500** 
- 节点ID是否需要重新映射（LINE目前只能支持节点ID属于一个连续的整数空间，最好在运行LINE之前做好ID的映射。如果设置为true，首先会做一次ID的映射并输出一个映射文件） **remapping:false**
- 使用的是一阶相似度还是二阶相似度（1或者2） **order:2**
- 每隔多少个epoch保存一次模型 **saveModelInterval:10**
- 每隔多少个epoch做一次checkpoint **checkpointInterval:1**
- 数据列分隔符 **sep:comma**

**性能**
- 模型初始化+全局alias table构建时间20分钟，每个epoch 50分钟

### 常见问题
  - 在差不多10min的时候，任务挂掉： 很可能的原因是angel申请不到资源！由于LINE基于Spark On Angel开发，实际上涉及到Spark和Angel两个系统，它们的向Yarn申请资源是独立进行的。 在Spark任务拉起之后，由Spark向Yarn提交Angel的任务，如果不能在给定时间内申请到资源，就会报超时错误，任务挂掉！ 解决方案是： 1）确认资源池有足够的资源 2） 添加spakr conf: spark.hadoop.angel.am.appstate.timeout.ms=xxx 调大超时时间，默认值为600000，也就是10分钟
  - 如何估算我需要配置多少Angel资源： 为了保证Angel不挂掉，需要配置模型大小两倍左右的内存。 模型大小的计算公式为： 节点数 * Embedding特征的维度 * order * 4 Byte，比如说1kw节点、100维、2阶的配置下，模型大小差不多有60G大小，那么配置instances=4, memory=30就差不多了。 
  - Spark的资源配置： Spark资源配置灵活，可以根据实际资源情况自由配置
