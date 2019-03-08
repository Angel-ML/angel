# K-CORE

> K-CORE(k-degenerate-graph)算法是重要的网络节点特征, 在复杂网络领域有广泛的应用。

## 1. 算法介绍
我们基于Spark On Angel实现了大规模网络的k-core计算，其中ps维护节点的最新coreness估计值和最后一次跟新对应的版本号。
Spark端维护网络的邻接表，每一轮拉取最新的coreness估计，根据网络节点的h-index定义，更新节点的coreness估计，再推送回ps。
算法在没有节点更新的时候停止。

## 2. 运行

### 参数

- input： hdfs路径，输入网络数据，每行两个长整形id表示的节点，以空白符或者逗号分隔，表示一条边
- output： hdfs路径， 输出节点对应的coreness， 每行一条数据，表示节点对应的coreness值，以tap符分割
- partitionNum： 输入数据分区数
- enableReIndex： 是否对数据网络节点进行从0开始的连续编号，如果输入数据本身符合要求可以设置为false
- switchRate： 计算模式切换比例，更新节点数的比例小于该值的时候，转换计算模式，默认为0.001，一般不需要修改
