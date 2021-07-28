# DataSampling
## 1. 算法介绍
该模块是一种常用的数据预处理方法，通常可作为其他算法的前提。它提供了从原数据集里随机抽取特定的比例或者特定数量的小样本的方法。
其他常见的算法模块可以通过配置抽样率完成数据抽样的功能，无需单独使用该模块；该模块常用于抽取小样本用于数据的可视化。<br>
说明：最终抽样的比例是min(抽样率， 抽样量/总数据量)。因此如果抽样量参数为1000，最终的抽样量不一定是精确的1000 <br>
算法不涉及ps相关资源
## 2. 运行
#### 算法IO参数

- input：输入，任何数据
- output: 输出，抽样后的数据，格式与输入数据一致
- sep: 数据分隔符，支持：空格(space)，逗号(comma)，tab(\t)
- featureCols：表示需要计算的特征所在列，例如“1-10,12,15”，其说明取特征在表中的第1到第10列，第12列以及第15列，从0开始计数

#### 算法参数
- sampleRate：样本抽样率
- takeSample：抽样数目，选填
- partitionNum：数据分区数，spark rdd数据的分区数量

#### 任务提交示例

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "DataSampling angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.DataSamplingExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output sep:tab partitionNum:4 \
  sampleRate:0.8 takeSample:25 \
  
```

# FillMissingValue
## 1. 算法介绍
该模块是对特征表中的空值进行填充，填充方式有4种：  <br>
1.missingValue，按照用户自定义值进行填充  <br>
2.mean，按照均值进行填充  <br>
3.median，按照中值进行填充  <br>
4.count，按照众数进行填充  <br>
算法不涉及ps相关资源
## 2. 运行
#### 算法IO参数

- input：输入，任何数据
- output: 输出，填充后的数据，格式与输入数据一致
- sep: 数据分隔符，支持：空格(space)，逗号(comma)，tab(\t)

#### 算法参数
- user-files：用户配置文件，定义缺失值填充方式
- fillStatPath：输出最终的缺失值，填充方式 + " " + lable col + ":" + 填充值，例如 <br>
count 0:1 <br>
median 1:0.5 <br>
missingValue 2:888 <br>

user-files户配置文件，例如
```
# 样例数据的json配置文件
 
{
    "feature": [
      {
        "id": "0",
        "fillMethod": "count"
      },
      {
        "id": "1",
        "fillMethod": "median"
       },
       {
        "id": "2-5",
        "missingValue": "888"
      }
    ]
}
```

#### 任务提交示例

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output
fillStatPath=hdfs://my-hdfs/fillStatPath

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "FillMissingValueExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --files ./localPath/FillMissingValueConf.txt \
  --class com.tencent.angel.spark.examples.cluster.FillMissingValueExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output fillStatPath:$fillStatPath sep:tab partitionNum:4 \
  user-files:FillMissingValueConf.txt \
  
```

# Spliter
## 1. 算法介绍
该模块是根据fraction数值将数据集分割为两部分，并将两部分分别存储  <br>
算法不涉及ps相关资源
## 2. 运行
#### 算法IO参数

- input：输入，任何数据
- output1: 分割后的数据1，格式与输入数据一致
- output2: 分割后的数据2，格式与输入数据一致
- sep: 数据分隔符，支持：空格(space)，逗号(comma)，tab(\t)

#### 算法参数
- fraction：数据分割比例，0.0-1.0之间的小数
- partitionNum：数据分区数，spark rdd数据的分区数量

#### 任务提交示例

```
input=hdfs://my-hdfs/data
output1=hdfs://my-hdfs/output1
output2=hdfs://my-hdfs/output2

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "SpliterExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.SpliterExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output1:$output1 output2:$output2 sep:tab partitionNum:4 \
  fraction:0.8 \
  
```

# Dummy
## 1. 算法介绍
Dummy模块中包含两个阶段，** 特征交叉 ** 和 ** 特征One-Hot ** 。** 特征交叉 ** 根据json配置文件，对指定的特征字段做交叉，生成feature name组成的特征； ** 特征One-Hot ** 将feature name编码成全局统一、连续的index。 <br>
算法不涉及ps相关资源

## 2. 输入数据
Dummy模块输入的数据是Table数据类型。  <br>
```
// 样例数据
1 0.5 2 1
0 0.4 3 2
1 0.5 1 1
```
说明：  <br>
数据中的 0 和 -1是两个特殊值，0表示默认值，-1 表示非法值；读取数据过程中将会过滤掉这两个特殊值；因此特征的数值表示应该避开0和-1。  <br>
支持多值特征。顾名思义，多值特征是指该特征可以包含多个value，每个value以“|”分割。例如，某特征是“喜欢的游戏”，该特征对应的值应该是多个游戏名，即游戏列表。  <br>
必须包含target字段。训练数据的target字段是0，1的label值，预测数据的target字段是每条数据的标识id。  <br>
支持标准的libsvm数据格式，第一列是label，index和value以冒号分割。不支持多值特征。  <br>
```
// libsvm数据格式样例
1 3:0.4 5:0.6 6:10
0 1:0.1 2: 10 3:0.5
```

## 3. 特征交叉
特征交叉的配置文件有两个对象“fields”和“feature_cross”，"fields"对象保存着输入数据每个字段对应的name和index；“feature_cross”对象是生成特征的配置，其中“id_features”是指生成单个特征，“comb_features”是指交叉的特征，dependencies是指交叉的特征，用逗号分隔，可以指定多个。  <br>
说明：  <br>
必须包含target字段  <br>
若某个特征是多值特征，"id_features"会生成多个特征，"comb_features"也会多次与其他的dependencies做交叉。  <br>
以下就是样例数据的配置和特征交叉阶段生成的中间结果。  <br>
```
# 样例数据的json配置文件
{
  "fields": [
    {
      "name": "target",
      "index": "0"
    },
    {
      "name": "f1",
      "index": "1"
    },
    {
      "name": "f2",
      "index": "2"
    },
    {
      "name": "f3",
      "index": "3"
    }
  ],
  "feature_cross": {
    "id_features": [
      {
        "name": "f1"
      },
      {
        "name": "f3"
      }
    ],
    "comb_features": [
      {
        "name": "f1_f2",
        "dependencies": "f1,f2"
      },
      {
        "name": "f2_f3",
        "dependencies": "f2,f3"
      }
    ]
  }
}

```

```
// 样例数据特征交叉后的中间结果
1 f1_0.5 f3_1 f1_f2_0.5_2 f2_f3_2_1
0 f1_0.4 f3_2 f1_f2_0.4_2 f2_f3_3_2
1 f1_0.5 f3_1 f1_f2_0.5_1 f2_f3_1_1
```

## 4. 特征One-Hot
特征One-Hot是基于特征交叉后的中间结果，将feature name字符串替换成全局统一、连续的feature index。   <br>
生成dummy格式的样本数据，每个样本以逗号分隔，第一个元素是target字段（训练数据的label或者预测数据的样本ID），
其他字段是指非0的feature index。   <br>
```
// one hot之后的结果
1,0,2,4,7
0,1,3,5,8
1,0,2,6,9
```

## 5. 运行
#### 算法IO参数

- input：输入，任何数据
- output: 输出，特征Dummy的输出包含三个目录，featureCount、featureIndexs、instances。输出路径，当instanceDir、indexDir、countDir不存在时，自动生成路径
- sep: 数据分隔符，支持：空格(space)，逗号(comma)，tab(\t)
- baseFeatIndexPath：指定了特征One-Hot时，使用的feature name到index之间的映射。属于选填参数。当需要基于上次特征Dummy的结果做增量更新或者预测数据的dummy，需要指定该参数，保证两次dummy的index是一致的。
- user-files: 配置文件
- instanceDir:保存了One-Hot后的dummmy格式的样本数据
- indexDir:保存了feature name到feature index的映射关系，中间用“:”分隔；
- countDir:保存了dummy后的特征空间维度，只保存了一个Int值，

#### 算法参数
- countThreshold：某个特征在整个数据集中出现的频次小于该阈值时，将会被过滤掉。一般设置成5左右
- negSampleRate:大部分应用场景中，负样本比例太大，可通过该参数对负样本采样
- partitionNum：数据分区数，spark rdd数据的分区数量

#### 任务提交示例

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "DummyExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.DummyExample \
  --files ./localPath/featConfPath \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output sep:tab partitionNum:4 user-files:featConfPath \
  negSampleRate:1 countThreshold:5 \
  
```

# Correlation
## 1. 算法介绍
该模块通过计算特征两两之间的pearson或者spearman相关系数，获得特征之间的相关性。该模块的相关性计算模式主要分为两种： <br>
一种是对相关性未知的特征（下文简称新特征）进行两两之间的相关性计算，结果为一个对角矩阵，每个对角矩阵的元素为两两特征之间的相关性系数，该值越大，说明这两个特征相关性越强；  <br>
另一种是将新特征与相关性已知的特征（下文简称旧特征）分别进行相关性计算。注意：当某列特征的方差为0时，与该特征计算的相关性值为NaN。 <br>
算法不涉及ps相关资源  <br>

输出：  <br>
1.如果没有指定新特征列，则模块不进行任何计算，也不输出任何结果  <br>
2.如果只指定了新特征列，没有指定旧特征列，则仅计算新特征两两之间的相关性，输出格式如下文例子中的（1）  <br>
3.如果既指定了新特征列，又指定了旧特征列，则模块不光会计算新特征两两之间的相关性，同时还会将新特征分别与旧特征列计算相关性，输出格式如下文例子中的（2） <br>
下面举几个例子说明上述情况：  <br>
(1) 如果只指定了新特征列:输出为新特征两两之间的相关性。例如新特征列为1,2,3，则输出格式为：  <br>

```
# 相关性系数输出样例
X 1 2 3
1 1.0 0.15 0.25
2 0.15 1.0 0.38
3 0.25 0.38 1.0
```
上面的数据显示总共有三个特征进行两两之间的相关性计算。第一行与第一列分别显示新特征的Id(X可忽略)，除此之外的其他元素分别为新特征两两之间的相关性系数。各元素之间以空格隔开。   <br>
(2) 新特征与旧特征均被指定:输出为两种相关性矩阵的合并，这两种矩阵分别是新特征两两之间的相关性矩阵和新特征与旧特征的相关性矩阵。例如新特征为1,2,3，旧特征列为4,5,6,7，则输出格式为：   <br>
```
# 相关性系数输出样例
X 1 2 3 4 5 6 7
1 1.0 0.15 0.25 0.57 0.15 0.25 0.02
2 0.15 1.0 0.38 0.15 0.11 0.38 0.49
3 0.25 0.38 1.0 0.25 0.38 0.03 0.21
```
上面的数据中第一行为新特征与旧特征的Id(X可忽略)，第一列为新特征Id，除此之外的其他元素为新特征两两之间以及新特征与旧特征之间的相关性系数。

## 2. 运行
#### 算法IO参数

- input：输入，特征输入文件
- output: 输出，相关性系数输出
- sep: 数据分隔符，支持：空格(space)，逗号(comma)，tab(\t)
- newColStr：新特征所在列，例如“1-10,12,15”，其说明取特征在表中的第1到第10列，第12列以及第15列，从0开始计数
- oriColStr：旧特征所在列，例如“1-10,12,15”，其说明取特征在表中的第1到第10列，第12列以及第15列，从0开始计数

#### 算法参数
- sampleRate：样本抽样率
- method：相关性计算方式，分为pearson和spearman两种,不填表示采用pearson方式
- partitionNum：数据分区数，spark rdd数据的分区数量

#### 任务提交示例

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "CorrelationExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.CorrelationExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output sep:tab partitionNum:4 \
  sampleRate:0.8 newColStr:1-5 oriColStr:7-9 method:pearson \
  
```

# MultualInformation
## 1. 算法介绍
该模块采用互信息公式计算特征之间的相关性，当值越大说明特征相关性越强。其中互信息的原理与计算公式可参考,同PearsonOrSpearman模块，MultualInformation模块的相关性计算主要分为两种：一是对新的特征进行两两之间的相关性计算，结果为一个对角矩阵，每个对角矩阵的元素为两两特征之间的相关性系数，该值越大，说明这两个特征相关性越强；二是将新特征与旧特征分别进行相关性计算。 <br>
输出：输出格式同Correlation的输出，但新特征两两之间的相关系数矩阵中对角线元素为该特征的信息熵，其他元素为特征之间的互信息。 <br>
算法不涉及ps相关资源
## 2. 运行
#### 算法IO参数

- input：输入，特征输入文件
- output: 输出，互信息输出
- sep: 数据分隔符，支持：空格(space)，逗号(comma)，tab(\t)
- newColStr：新特征所在列，例如“1-10,12,15”，其说明取特征在表中的第1到第10列，第12列以及第15列，从0开始计数
- oriColStr：旧特征所在列，例如“1-10,12,15”，其说明取特征在表中的第1到第10列，第12列以及第15列，从0开始计数

#### 算法参数
- sampleRate：样本抽样率
- takeSample：抽样数目，选填
- partitionNum：数据分区数，spark rdd数据的分区数量

#### 任务提交示例

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "MutualInformationExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.MutualInformationExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output sep:tab partitionNum:4 \
  sampleRate:0.8 newColStr:1-5 oriColStr:7-9
  
```
