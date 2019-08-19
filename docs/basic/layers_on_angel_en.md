
# Layers in Angel    

Most of the algorithms in Angel are based on [computational graph](./computational_graph_on_angel.md). The nodes in Angel are layers. According to the topological structure of layers, they can be divided into three categories:
- verge: Edge nodes, with only input or output layers, such as input and loss layers
    - input layer: Mainly has SimpleInputLayer, Embedding
    - Loss layer: Mainly using SimpleLossLayer, SoftmaxLossLayer
- linear: Layer with one input and one output
    - Fully connected layer: that is FCLayer
    - Feature intersection layer: There are many such layers, different algorithms use different feature crossing methods, and can also be used in combination. The main ones are::
        - BiInnerCross: Feature crossing method used in PNN(inner)
        - BiOutterCross: Feature crossing method used in PNN(outter)(Currently not implemented)
        - BiInnerSumCross: Second-order feature implicit crossover method used in FM
        - BiIntecationCross: Feature crossing method used in NFM
- join: There are two or more inputs, one output layer, and there are many such layers, mainly:
    - ConcatLayer: Put together multiple input layers and enter a Dense matrix
    - SumPooling: Add the input elements correspondingly and output them
    - MulPooling: Multiply input elements to output
    - DotPooling: Multiply the corresponding elements first, then add them by row, and enter a matrix of n rows and one column.


## 1. Input Layer
There are two types of input layers in Angel:
- SimpleInputLayer
- Embedding

### 1.1 SimpleInpuyLayer
As the name implies, it accepts input. Class constructors are as follows:
```scala
class SimpleInputLayer(name: String, outputDim: Int, transFunc: TransFunc, override val optimizer: Optimizer)(implicit graph: AngelGraph)
  extends InputLayer(name, outputDim)(graph) with Trainable
```
Its main features are:
- Receiving Dense/Sparse Input
- When the input is dense, the internal parameters are dense, the parameters are stored in an array continuously, and the BLAS library is called to complete the calculation; when the input is sparse, the internal parameters are stored in RowBasedMatrix, each row is a sparse vector, and Angel internal mathematical library is used for calculation.
- You need to specify outputDim to specify the transfer function and optimizer

The completed calculation is expressed as follows:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(x)=tranfunc(x\bold{w}+bias))

A typical JSON expression is:
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
Embedding is common to many deep learning algorithms. The constructors of the class are as follows:
```scala
class Embedding(name: String, outputDim: Int, val numFactors: Int, override val optimizer: Optimizer)(implicit graph: AngelGraph)
  extends InputLayer(name, outputDim)(graph) with Trainable
```
In addition to name and optimizer, the other two parameters of Embedding are as follows:
- outputDim: It refers to the output of lookup. Angel's Embedding currently assumes that each sample has the same number of fields, and each field is One-hot. The first condition is true in most cases, but the second condition is more Strict, in some cases not established, will be relaxed in the future
- numFactors: It refers to the dimension of the Embedding vector. The size of the Embedding matrix is obtained as follows: The dimension of the input data is stored in the system. This dimension is obtained as the number of columns of the Embedding matrix, and numFactors is the number of rows of the Embedding matrix ( Note: Although the internal implementation is a bit different, this understanding is ok)

Embedding is a table in the abstract sense, and provides a lookup table (lookup/calOutput). Angel's Embedding is special in that it allows some operations after checking the table, so it consists of two steps:
- Look up table: According to the index, find the corresponding column in the table
- Calculation assembly: Sometimes the data is not one-hot, multiply the found vector by a value.

The results of sparse vector embedding with values of 1 (one-hot encoding, expressed in dummy format) and floating point (expressed in libsvm format) are shown below:

![model](http://latex.codecogs.com/png.latex?\dpi{120}(1,5,40,\cdots,10000)\rightarrow(\bold{v}_1,\bold{v}_5,\bold{v}_{40},\cdots,\bold{v}_{10000}))

![model](http://latex.codecogs.com/png.latex?\dpi{120}(1:0.3,5:0.7,40:1.9,\cdots,10000:3.2)\rightarrow(0.3\bold{v}_1,0.7\bold{v}_5,1.9\bold{v}_{40},\cdots,3.2\bold{v}_{10000}))

A typical JSON expression is:
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

## 2. Linear Layer
A linear layer is a layer that has one input and one output. It mainly includes a fully coupled layer (FCLayer) and a series of feature crossing layers.

### 2.1 FCLayer
FCLayer layer is the most common layer in DNN. Its calculation can be expressed by the following formula:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(x)=tranfunc(x\bold{w}+bias))

The constructor in Angel is as follows:
```scala
class FCLayer(name: String, outputDim: Int, inputLayer: Layer, transFunc: TransFunc, override val optimizer: Optimizer
             )(implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph) with Trainable 
```
From the constructor and calculation formula, it is very similar to DenseInputLayer/SparseInputLayer. The difference is that the input of the former is a Layer, and the latter directly inputs data (do not specify the input Layer in the constructor).

In parameter data storage, FCLayer, like DenseInput Layer, also uses dense method to calculate with BLAS.

Since FCLayer is usually used in multiple stacks, there are some simplifications in the configuration of the data, that is, the parameter reduction of multiple stacked FCLayer. Here is an example:
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

There are three FCLayer stacked together, the input is the input of the first layer, the output is the output of the last layer, the outputdim of each layer, and the transfunc is represented by the column, namely:
- outputdims: The output Dim of each FCLayer is given in the form of a list.
- transfuncs: Give each FCLayer's transfunc as a list

Note: You can also specify an optimizer for the superimposed FCLayer. At this point, all layers have the same optimizer. If you write separately, it is:
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
The formula for calculating the feature crossing layer is as follows:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)=\sum_i^k\sum_{j=i+1}^k\bold{u}_i^T\bold{u}_j)

Where ![](http://latex.codecogs.com/png.latex?(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)) is the output of Embedding. Specifically, the Embedding result is a two-in-one inner product, and then summed. Therefore, BiInnerSumCross has no parameters, is untrainable, and has an output dimension of 1.

The constructor is as follows:
```scala
class BiInnerSumCross(name: String, inputLayer: Layer)(
  implicit graph: AngelGraph) extends LinearLayer(name, 1, inputLayer)(graph)
```

An example of the json parameter is as follows:
```json
{
    "name": "biinnersumcross",
    "type": "BiInnerSumCross",
    "inputlayer": "embedding",
    "outputdim": 1
},
```


### 2.3 BiInnerCross
The formula for calculating the feature crossing layer is as follows:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)=(\bold{u}_1^T\bold{u}_2,\bold{u}_1^T\bold{u}_3,\bold{u}_1^T\bold{u}_4,\cdots,\bold{u}_{k-1}^T\bold{u}_k))

Note：![](http://latex.codecogs.com/png.latex?(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k))is the output of Embedding. Specifically, Embedding results do two or two inner products, so the dimension of output is![](http://latex.codecogs.com/png.latex?\dpi{60}C_k^2=\frac{k(k-1)}{2}). It can be seen that BiInnerCross has no parameters, is untrainable, and has an output dimension of ![](http://latex.codecogs.com/png.latex?\dpi{80}C_k^2).

The constructor is as follows:
```scala
class BiInnerCross(name: String, outputDim: Int, inputLayer: Layer)(
  implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph) 
```

An example of the json parameter is as follows:
```json
{
    "name": "biInnerCross",
    "type": "BiInnerCross",
    "outputdim": 78,
    "inputlayer": "embedding"
},
```

### 2.4 BiInteactionCross
The formula for calculating the feature crossing layer is as follows:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)=\sum_i^k\sum_{j=i+1}^k\bold{u}_i\otimes\bold{u}_j)

Notes: ![](http://latex.codecogs.com/png.latex?(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k))is the output result of Embedding. Specifically, the result of Embedding is the product of two corresponding elements![](http://latex.codecogs.com/png.latex?\bold{u}_i\otimes\bold{u}_j), and then the sum, so the dimension of output is the same as ![](http://latex.codecogs.com/png.latex?\bold{u}_k), and has nothing to do with the dimension of input data. Thus, BiInteaction Cross has no parameters and is untrainable.

The constructor is as follows:
```scala
class BiInteractionCross(name: String, outputDim: Int, inputLayer: Layer)(
  implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph)
```

An example of the json parameter is as follows:
```json
{
    "name": "biinteractioncross",
    "type": "BiInteractionCross",
    "outputdim": 8,
    "inputlayer": "embedding"
},
```
## 3. Join Layer
The join layer refers to a layer with multiple inputs and one output, mainly including:
- ConcatLayer: Combine multiple input layers and enter a Dense matrix
- SumPooling: Add the input elements correspondingly and output them
- MulPooling: Multiply input elements to output
- DotPooling: Multiply the corresponding elements first, then add them by row, and output a matrix of n rows and one column

### 3.1 ConcatLayer
Combine multiple input layers and enter a Dense matrix. The constructor is as follows:
```scala
class ConcatLayer(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph)
```

An example of the json parameter is as follows:
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
There are multiple input layers, specified by inputlayers, in the form of a list.

### 3.2 SumPoolingLayer
The input elements are added together to output a Dense matrix. The constructor is as follows:
```scala
class SumPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph)
```

An example of the json parameter is as follows:
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
There are multiple input layers, which are specified in the form of lists, using inputlayers.

### 3.3 MulPoolingLayer
After multiplying the input elements, a Dense matrix is output. The constructor is as follows:
```scala
class MulPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph)
```

An example of the json parameter is as follows:
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
There are multiple input layers, which are specified in the form of lists, using inputlayers.

### 3.4 DotPoolingLayer
First multiply the corresponding elements, then add them by row, and output a matrix of n rows and columns. The constructor is as follows:
```scala
class DotPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph)
```

An example of the json parameter is as follows:
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
There are multiple input layers, which are specified in the form of lists, using inputlayers.

## 4. Loss layer
At the top of the network, there is only the input layer, there is no output layer, used to calculate the loss. For the loss function, please refer to [Angel中的损失函数](./lossfunc_on_angel.md)

### 4.1 SimpleLossLayer
The constructor of SimpleLossLayer is as follows:
```scala
class SimpleLossLayer(name: String, inputLayer: Layer, lossFunc: LossFunc)(
  implicit graph: AngelGraph) extends LinearLayer(name, 1, inputLayer)(graph) with LossLayer
```

An example of the json parameter is as follows:
```json
{
    "name": "simplelosslayer",
    "type": "Simplelosslayer",
    "lossfunc": "logloss",
    "inputlayer": "sumPooling"
}
```