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
  ../lib/spark-on-angel-examples-3.2.0.jar
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
  ../lib/spark-on-angel-examples-3.2.0.jar
  input:$input output:$output fillStatPath:$fillStatPath sep:tab partitionNum:4 \
  user-files:FillMissingValueConf.txt \
  
```

# Spliter
## 1. 算法介绍
该模块是根据fraction数值将数据集分割为两部分，并将两部分分别存储
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
  ../lib/spark-on-angel-examples-3.2.0.jar
  input:$input output1:$output1 output2:$output2 sep:tab partitionNum:4 \
  fraction:0.8 \
  
```
