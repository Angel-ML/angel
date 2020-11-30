# Layers on Angel

Most of the algorithms in Angel are based on [computational graphs](./computinggraph_on_angel.md), in which a node represents a layer. These layers can be separated into three classes basing on their topological structure:

- verge: edge node, only exists in input or output layer, such as inputLayer and lossLayer
    - InputLayer: mainly SimpleInputLayer and Embedding
    - LossLayer: mainly SimpleLossLayer and SoftmaxLossLayer
- linear: layer with only one input and one output
    - Fully connected layer: i.e. FCLayer
    - Feature crossn: there are many such layers. Different algorithms use different feature cross method or combinations of several crosses:
        - BiInnerCross: the feature cross used in PNN(inner)
        - BiOutterCross: the feature cross used in PNN(outter) (not implemented yet)
        - BiInnerSumCross: implicit cross of second-order features used in FM
        - BiIntecationCross: the feature cross used in NFM
- join: layer with two or more input and one output. There are also many types of such layers:
    - ConcatLayer: concat multiple input layers to output a Dense matrix
    - SumPooling: sum up the input elements and output the result
    - MulPooling: multiply the input elements and output the result
    - DotPooling: multiply the corresponding elements then sum up by row, and output a matrix with n rows and one column


## 1. Input Layer
There are two types of input layers in Angel:
- SimpleInputLayer
- Embedding

### 1.1 SimpleInputLayer
As its name implies, `SimpleInputLayer` receives input data. The constructor of this class is as follows:
```scala
class SimpleInputLayer(name: String, outputDim: Int, transFunc: TransFunc, override val optimizer: Optimizer)(implicit graph: AngelGraph)
  extends InputLayer(name, outputDim)(graph) with Trainable
```
`SimpleInputLayer` has following characteristics:

- Receive dense/sparse input
- When the input is dense, the internal parameters will be dense, stored in an array in succession, and calculated using the BLAS library; When the input is sparse, the internal parameters will be stored in a `RowBasedMatrix`, in which each line is a sparse vector, and the parameters are calculated using Angel's internal math library
- Its outputDim should be specified. Users can also specify its transfer function and optimizer

The calculation formula is:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(x)=tranfunc(x\bold{w}+bias))

A typical Json expression is:
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
Embedding is common to many deep learning algorithms. The constructor of `Embedding` class is as follows:

```scala
class Embedding(name: String, outputDim: Int, val numFactors: Int, override val optimizer: Optimizer)(implicit graph: AngelGraph)
  extends InputLayer(name, outputDim)(graph) with Trainable
```
The two parameters except `name` and `optimizer` are:

- outputDim: the dimension of the output of lookup. Angel's Embedding currently supposes all samples have the same number of fields and each field is in one hot encoding. The first condition is true in most cases, but the second is more stringent and is not true in some cases. This restriction will be relaxed in future version
- numFactors: the dimension of the Embedding vector. The size of an Embedding matrix is obtained as follows: the system stores the dimension of input data, which will be exactly the column number of Embedding matrix; The numFactors is the row number of the matrix (Note: the internal implementation is a bit different, but it can basically be understood like this)

Embedding can be abstractly regarded as a table that provides lookup methods (lookup/calOutput). Angel's Emgedding is special in that it allows some operations after looking up the table. Thus there are two steps in total:

- lookup: find the corresponding column according to index
- calculation and assembly: in some cases, the data might not be one hot, so the resulting vector should be multiplied by a specific value

Following is the embedding results of a sparse vector when value is 1 (result of one hot encoding, represented by dummy format) and when value is float (represented by libsvm format):

![model](http://latex.codecogs.com/png.latex?\dpi{120}(1,5,40,\cdots,10000)\rightarrow(\bold{v}_1,\bold{v}_5,\bold{v}_{40},\cdots,\bold{v}_{10000}))

![model](http://latex.codecogs.com/png.latex?\dpi{120}(1:0.3,5:0.7,40:1.9,\cdots,10000:3.2)\rightarrow(0.3\bold{v}_1,0.7\bold{v}_5,1.9\bold{v}_{40},\cdots,3.2\bold{v}_{10000}))

A typical expression in Json is:
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

## 2. Linear Layers
Linear layers are layers with only one input and one output, mainly consisting of fully connected layer (FCLayer) and a series of feature crossing layers.

### 2.1 FCLayer
FCLayer is the most common layer in DNN. Its calculation can be expressed by:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(x)=tranfunc(x\bold{w}+bias))

The constructor of `FCLayer` in Angel is:
```scala
class FCLayer(name: String, outputDim: Int, inputLayer: Layer, transFunc: TransFunc, override val optimizer: Optimizer
             )(implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph) with Trainable 
```
As we can see from the constructor and formula, FCLayer is quite similar to DenseInputLayer/SparseInputLayer, except that the input of a FCLayer is a layer, while the input of the latter is data (not specifying the input layer in the constructor).

For the parameter storage, FCLayer, like DenseInputLayer, also uses a dense method and is calculated using BLAS.

Since FCLayers are usually used when being stacked together, Angel makes some simplifications in data configuration, that is, reduces the parameters of multiple stacked layers. Following is an example:

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
There are three FCLayers stacking together. The input layer is the input of the first layer, and the output should be that of the last layer. Each layer's outputDim and transfunc are represented by a column, that is:

- outputdims: specifies each FCLayer's outputDim in list format
- transfuncs: specifies each FCLayer's transfunc in list format

Note: user can specify optimizer for stacked FCLayers. In this way, all layers share the same type of optimizer. The Json configuration will be as follows if FCLayers are specified separately:

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
Feature cross layer. The calculation formula is:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)=\sum_i^k\sum_{j=i+1}^k\bold{u}_i^T\bold{u}_j)

in which the ![](http://latex.codecogs.com/png.latex?(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)) is the output of Embedding layer. Specifically, this formula sums up the inner products of all possible pair combinations of elements in Embedding output. Therefore, `BilnnerSumCross` has no parameter and is untrainable. The output dimension is 1.

The constructor of `BilnnerSumCross` is:
```scala
class BiInnerSumCross(name: String, inputLayer: Layer)(
  implicit graph: AngelGraph) extends LinearLayer(name, 1, inputLayer)(graph)
```

An Json example:
```json
{
    "name": "biinnersumcross",
    "type": "BiInnerSumCross",
    "inputlayer": "embedding",
    "outputdim": 1
},
```


### 2.3 BiInnerCross
Feature cross layer. The calculation formula is:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)=(\bold{u}_1^T\bold{u}_2,\bold{u}_1^T\bold{u}_3,\bold{u}_1^T\bold{u}_4,\cdots,\bold{u}_{k-1}^T\bold{u}_k))

in which the ![](http://latex.codecogs.com/png.latex?(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)) is the output of Embedding layer. Specifically, the Bilnner Cross computes the inner products of all possible pair combination of elements in the Embedding output, thus the output dimension is ![](http://latex.codecogs.com/png.latex?\dpi{60}C_k^2=\frac{k(k-1)}{2}). Therefore, BiInnerCross also has no parameter and is untrainable, and the output dimension is ![](http://latex.codecogs.com/png.latex?\dpi{80}C_k^2).

The constructor of `BilnnerCross` class is:
```scala
class BiInnerCross(name: String, outputDim: Int, inputLayer: Layer)(
  implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph) 
```

A Json example:
```json
{
    "name": "biInnerCross",
    "type": "BiInnerCross",
    "outputdim": 78,
    "inputlayer": "embedding"
},
```

### 2.4 BiInteactionCross
Feature cross layer. The calculation formula is:

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)=\sum_i^k\sum_{j=i+1}^k\bold{u}_i\otimes\bold{u}_j)

in which the ![](http://latex.codecogs.com/png.latex?(\bold{u}_1,\bold{u}_2,\cdots,\bold{u}_k)) is the output of Embedding layer. Bi-interaction cross computes the element-wise products ![](http://latex.codecogs.com/png.latex?\bold{u}_i\otimes\bold{u}_j) of all combinations of elements in Embedding vector, and then sum them up. Hence the output dimension is identical to the dimension of ![](http://latex.codecogs.com/png.latex?\bold{u}_k), and is independent of dimension of input data. Hence, BiInteactionCross also has no parameter and is untrainable.

The constructor of `BiInteractionCross` is:
```scala
class BiInteractionCross(name: String, outputDim: Int, inputLayer: Layer)(
  implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph)
```

A json example:
```json
{
    "name": "biinteractioncross",
    "type": "BiInteractionCross",
    "outputdim": 8,
    "inputlayer": "embedding"
},
```
## 3. Join Layers
Join layers are those layers with multiple input and one output, mainly including:

- ConcatLayer: concat multiple input layers to output a Dense matrix
- SumPooling: sum up the input elements and output the result
- MulPooling: multiply the input elements and output the result
- DotPooling: multiply the corresponding elements then sum up by row, and output a matrix with n rows and one column

### 3.1 ConcatLayer
Concat multiple input layers to output a Dense matrix. The constructor is:
```scala
class ConcatLayer(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph)
```

An example of Json parameters:
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
Since there exist multiple input layers, they should be indicated by "inputlayers" in list format.

### 3.2 SumPoolingLayer
Sum up the input elements and output the result. The constructor is:
```scala
class SumPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph)
```

An example of Json parameters:
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
Since there exist multiple input layers, they should be indicated by "inputlayers" in list format.

### 3.3 MulPoolingLayer
Multiply the input elements and output the result. The constructor is:
```scala
class MulPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph)
```

An example of Json parameters:
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
Since there exist multiple input layers, they should be indicated by "inputlayers" in list format.

### 3.4 DotPoolingLayer
Multiply the corresponding elements then sum up by row, and output a matrix with n rows and one column. The constructor is:
```scala
class DotPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph)
```

An example of Json parameters:
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
Since there exist multiple input layers, they should be indicated by "inputlayers" in list format.

## 4. Loss Layers
A loss layer should be the top layer of a network with only input layer and no output layer. LossLayer is used to compute the loss. Please refer to [Loss functions in Angel](./lossfunc_on_angel.md) for loss functions.

### 4.1 SimpleLossLayer
The constructor of SimpleLossLayer is:
```scala
class SimpleLossLayer(name: String, inputLayer: Layer, lossFunc: LossFunc)(
  implicit graph: AngelGraph) extends LinearLayer(name, 1, inputLayer)(graph) with LossLayer
```

An example of Json parameters:
```json
{
    "name": "simplelosslayer",
    "type": "Simplelosslayer",
    "lossfunc": "logloss",
    "inputlayer": "sumPooling"
}
```