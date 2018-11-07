# Angel中的层

Angel中的大部分算法都是基于[计算图](./computinggraph_on_angel.md)的, 图中的节点为层(layer). 按层的拓朴结构可分为三类:
- verge: 边缘节点, 只有输入或输出的层, 如输入层与损失层
    - 输入层: 主要有SimpleInputLayer, Embedding
    - 损失层: 主要用SimpleLossLayer, SoftmaxLossLayer
- linear: 有且仅有一个输入与一个输出的层
    - 全连接层: 即FCLayer
    - 特征交叉层: 这类层较多, 不同的算法使用不同的特征交叉方式, 也可以组合使用, 主要有:
        - BiInnerCross: PNN(inner)中使用的特征交叉方式
        - BiOutterCross: PNN(outter)中使用的特征交叉方式(目前未实现)
        - BiInnerSumCross: FM中使用的二阶特征隐式交叉方式
        - BiIntecationCross: NFM中使用的特征交叉方式
- join: 有两个或多个输入, 一个输出的层, 这类层也较多, 主要有:
    - ConcatLayer: 将多个输入层拼起来, 输入一个Dense矩阵
    - SumPooling: 将输入元素对应相加后输出
    - MulPooling: 将输入元素对应相乘后输出
    - DotPooling: 先将对应元素相乘, 然后按行相加, 输入n行一列的矩阵


## 1. 输入层
Angel中的输入层有两类:
- SimpleInputLayer
- Embedding

### 1.1 SimpleInpuyLayer
顾名思义, 它是接受输入的. 类的构造函数如下:
```scala
class SimpleInputLayer(name: String, outputDim: Int, transFunc: TransFunc, override val optimizer: Optimizer)(implicit graph: AngelGraph)
  extends InputLayer(name, outputDim)(graph) with Trainable
```
它的主要特点为:
- 接收稠密/稀疏输入
- 当输入是稠密时, 内部参数是稠密的, 参数连续存储于一个数组, 调用BLAS库完成计算; 当输入是稀疏时, 内部参数用RowBasedMatrix存储, 每行都是一个稀疏向量, 计算用Angel内部数学库
- 需要指定outputDim, 可以指定传输函数和优化器

完成的计算用公式表达为:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(x)=tranfunc(x\bold{w}+bias))

一种典型的json表达为:
```json
{
    "name": "wide",
    "type": "Simpleinputlayer",
    "outputdim": 10,
    "transfunc": "identity",
    "optimizer": "adam"
},
```

### 1.2 Embedding
Embedding是很多深度学习算法共有的. 类的构造函数如下:
```scala
class Embedding(name: String, outputDim: Int, val numFactors: Int, override val optimizer: Optimizer)(implicit graph: AngelGraph)
  extends InputLayer(name, outputDim)(graph) with Trainable
```
除了name与optimizer这两个参数据外, Embedding的其它两个参数如下:
- outputDim: 是指lookup的输出, Angel的Embedding目前假设每个样本都具有相同的field数目, 每个field都是One-hot的. 第一个条件在大部分情况下都成立, 但是第二个条件较为严格, 有些情况下并不成立, 以后会放宽这一限制
- numFactors: 是指Embedding向量的维数. 关于Embedding矩阵的大小是这样获得的: 系统中存有输入数据的维数, 这个维数被获取后成为Embedding矩阵的列数, 而numFactors就是Embedding矩阵的行数(注: 虽然内部实现上有点不同, 但这样理解是可以的)

Embedding在抽象意义上是一张表, 并提供查表的方法(lookup/calOutput). Angel的Embedding的特别之处在于查完表后还允许有一些运算, 所以包括两个步骤:
- 查表: 根据索引, 到表中查出相应的列
- 计算组装: 有时数据不是one-hot, 要将查得的向量乘以一个值

下面展示值为1(one-hot编码的结果, 用dummy格式表示)与值为浮点数(用libsvm格式表示)两种情况下稀疏向量embedding的结果:

![model](http://latex.codecogs.com/png.latex?\dpi{120}(1,5,40,\cdots,10000)\rightarrow(\bold{v}_1,\bold{v}_5,\bold{v}_{40},\cdots,\bold{v}_{10000}))

![model](http://latex.codecogs.com/png.latex?\dpi{120}(1:0.3,5:0.7,40:1.9,\cdots,10000:3.2)\rightarrow(0.3\bold{v}_1,0.7\bold{v}_5,1.9\bold{v}_{40},\cdots,3.2\bold{v}_{10000}))

一种典型的json表达为:
```json
{
    "name": "embedding",
    "type": "Embedding",
    "numfactors": 8,
    "outputdim": 104,
    "optimizer": {
        "type": "momentum",
        "momentum": 0.9,
        "reg2": 0.01
    }
},
```

## 2. 线性层
线性层是指有且只有一个输入一个输出的层. 主要包括全联接层(FCLayer)和一系列的特征交叉层.

### 2.1 FCLayer
FCLayer层是DNN中最常见的层, 其计算可用下面的公式表达:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(x)=tranfunc(x\bold{w}+bias))

在Angel中的构造函数如下:
```scala
class FCLayer(name: String, outputDim: Int, inputLayer: Layer, transFunc: TransFunc, override val optimizer: Optimizer
             )(implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph) with Trainable 
```
从构造函数与计算公式可知, 它与DenseInputLayer/SparseInputLayer十分相似, 有所不同的是前者的输入是一个Layer, 后者直接输入数据(在构造函数中不要指定输入Layer). 

在参数据存储上, FCLayer与DenseInputLayer一样, 也使用稠密的方式, 用BLAS计算.

由于FCLayer通常是多个叠在一起使用, 在参数据配置方面做了一些简化, 即将多个叠在一起的FCLayer的参数约简, 下面是一个例子:
```json
{
    "name": "fclayer",
    "type": "FCLayer",
    "outputdims": [
        100,
        100,
        1
    ],
    "transfuncs": [
        "relu",
        "relu",
        "identity"
    ],
    "inputlayer": "embedding"
},
```
这里有三个FCLayer叠在一起, 输入是第一层的输入, 输出为最后一层的输出, 每一层的outputdim, transfunc用列表示, 即:
- outputdims: 以列表的形式给出每个FCLayer的outputDim
- transfuncs: 以列表的形式给出每个FCLayer的transfunc

注: 也可以为叠合的FCLayer指定optimizer, 此时, 所有layer都有相同的optimizer. 如果分开写则为:
```json
{
    "name": "fclayer_0",
    "type": "FCLayer",
    "outputdim": 100,
    "transfuncs": "relu",
    "inputlayer": "embedding"
},
{
    "name": "fclayer_1",
    "type": "FCLayer",
    "outputdim": 100,
    "transfuncs": "relu",
    "inputlayer": "fclayer_0"
},
{
    "name": "fclayer",
    "type": "FCLayer",
    "outputdim": 1,
    "transfuncs": "identity",
    "inputlayer": "fclayer_1"
},
```

### 2.2 BiInnerSumCross
特征交叉层, 计算公式如下:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)=\sum_i^k\sum_{j=i+1}^k\bold{u}_i^T\bold{u}_j)

其中![](http://latex.codecogs.com/png.latex?(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k))是Embedding的输出结果. 具体而言是Embedding结果做两两内积, 再求和. 因此BiInnerSumCross没有参数, 是untrainable的, 输出维度为1.

构造函数如下:
```scala
class BiInnerSumCross(name: String, inputLayer: Layer)(
  implicit graph: AngelGraph) extends LinearLayer(name, 1, inputLayer)(graph)
```

json参数例子如下:
```json
{
    "name": "biinnersumcross",
    "type": "BiInnerSumCross",
    "inputlayer": "embedding",
    "outputdim": 1
},
```


### 2.3 BiInnerCross
特征交叉层, 计算公式如下:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)=(\bold{u}_1^T\bold{u}_2,\bold{u}_1^T\bold{u}_3,\bold{u}_1^T\bold{u}_4,\cdots,\bold{u}_{k-1}^T\bold{u}_k))

其中![](http://latex.codecogs.com/png.latex?(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k))是Embedding的输出结果. 具体而言是Embedding结果做两两内积, 因此输出的维度为![](http://latex.codecogs.com/png.latex?\dpi{60}C_k^2=\frac{k(k-1)}{2}). 由此可见, BiInnerCross也是没有参数, 是untrainable的, 输出维度为![](http://latex.codecogs.com/png.latex?\dpi{80}C_k^2).

构造函数如下:
```scala
class BiInnerCross(name: String, outputDim: Int, inputLayer: Layer)(
  implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph) 
```

json参数例子如下:
```json
{
    "name": "biInnerCross",
    "type": "BiInnerCross",
    "outputdim": 78,
    "inputlayer": "embedding"
},
```

### 2.4 BiInteactionCross
特征交叉层, 计算公式如下:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)=\sum_i^k\sum_{j=i+1}^k\bold{u}_i\otimes\bold{u}_j)

其中![](http://latex.codecogs.com/png.latex?(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k))是Embedding的输出结果. 具体而言是Embedding结果做两两对应元素积![](http://latex.codecogs.com/png.latex?\bold{u}_i\otimes\bold{u}_j), 再相和, 因此输出的维度为与![](http://latex.codecogs.com/png.latex?\bold{u}_k)相同, 与输入数据的维度元关. 由此可见, BiInteactionCross也是没有参数, 是untrainable的.

构造函数如下:
```scala
class BiInteractionCross(name: String, outputDim: Int, inputLayer: Layer)(
  implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph)
```

json参数例子如下:
```json
{
    "name": "biinteractioncross",
    "type": "BiInteractionCross",
    "outputdim": 8,
    "inputlayer": "embedding"
},
```
## 3. Join层
join层是指有多个输入一个输出的层, 主要有:
- ConcatLayer: 将多个输入层拼接起来, 输入一个Dense矩阵
- SumPooling: 将输入元素对应相加后输出
- MulPooling: 将输入元素对应相乘后输出
- DotPooling: 先将对应元素相乘, 然后按行相加, 输出n行一列的矩阵

### 3.1 ConcatLayer
将多个输入层拼接起来, 输入一个Dense矩阵, 构造函数如下:
```scala
class ConcatLayer(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph)
```

json参数例子如下:
```json
{
    "name": "concatlayer",
    "type": "ConcatLayer",
    "outputdim": 182,
    "inputlayers": [
        "embedding",
        "biInnerCross"
    ]
},
```
有多个输入层, 用inputlayers, 以列表的形式指定.

### 3.2 SumPoolingLayer
将输入元素对应相加后输出, 输出一个Dense矩阵, 构造函数如下:
```scala
class SumPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph)
```

json参数例子如下:
```json
{
    "name": "sumPooling",
    "type": "SumPooling",
    "outputdim": 1,
    "inputlayers": [
        "wide",
        "fclayer"
    ]
},
```
有多个输入层, 用inputlayers, 以列表的形式指定.

### 3.3 MulPoolingLayer
将输入元素对应相乘后输出, 输出一个Dense矩阵, 构造函数如下:
```scala
class MulPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph)
```

json参数例子如下:
```json
{
    "name": "mulPooling",
    "type": "MulPooling",
    "outputdim": 1,
    "inputlayers": [
        "wide",
        "fclayer"
    ]
},
```
有多个输入层, 用inputlayers, 以列表的形式指定.

### 3.4 DotPoolingLayer
先将对应元素相乘, 然后按行相加, 输出n行一列的矩阵, 构造函数如下:
```scala
class DotPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph)
```

json参数例子如下:
```json
{
    "name": "dotPooling",
    "type": "DotPooling",
    "outputdim": 1,
    "inputlayers": [
        "wide",
        "fclayer"
    ]
},
```
有多个输入层, 用inputlayers, 以列表的形式指定.

## 4. 损失层
在网络的最上层, 只有输入层, 没有输出层, 用于计算损失. 关于损失函数, 请参考[Angel中的损失函数](./lossfunc_on_angel.md)

### 4.1 SimpleLossLayer
SimpleLossLayer的构造函数如下:
```scala
class SimpleLossLayer(name: String, inputLayer: Layer, lossFunc: LossFunc)(
  implicit graph: AngelGraph) extends LinearLayer(name, 1, inputLayer)(graph) with LossLayer
```

json参数例子如下:
```json
{
    "name": "simplelosslayer",
    "type": "Simplelosslayer",
    "lossfunc": "logloss",
    "inputlayer": "sumPooling"
}
```