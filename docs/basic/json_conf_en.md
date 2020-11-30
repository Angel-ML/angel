# Json Configuration Files

Since deep learning often comes with a large number of parameters, it is not convenient to directly specify parameters using scripts. Beginning from the 2.0 veresion, Angel now supports specifying model parameters in Json files (Note: system parameters are still configured by scripts). Json files have currently two major purposes:

- Provide parameters for existing algorithms, including:
    + DeepFM
    + PNN
    + NFM
    + Deep and Wide
- Customize a new network using Json, similar to Caffe

Parameters that can be configured in Json file include:

- Data-related parameters
- Model-related parameters
- Training-related parameters
- Default optimizer, transfer function, loss function
- Layers in network

The following will introduce these parameters one by one.

## 1. Data-Related Parameters
A whole data configuration will be like:

```json
"data": {
    "format": "dummy",
    "indexrange": 148,
    "numfield": 13,
    "validateratio": 0.1,
    "sampleratio": 0.2,
    "useshuffle": true,
    "translabel": "NoTrans",
    "posnegratio": 0.01
}
```
The Json keys are demonstrated in the table below:

Json key | Corresponding configuration item | Description 
---|---|---
format | ml.data.type | Type of input data, can be dense, dummy (default) or libsvm 
indexrange | ml.feature.index.range | Feature dimension 
numfield | ml.fm.field.num | Number of fields in the input data. Although feature crosses can generate extremely high-dimensional data, the number of fields of each sample is fixed for some data sets 
validateratio | ml.data.validate.ratio | Angel will partition input data into training set and validation set. This parameter specifies the ratio of validation set. Note: all validation sets are stored in memory, so there could be OOM when the data size is very large 
sampleratio | ml.batch.sample.ratio | This parameter is dedicated to Spark on Angel. Spark generates mini-batches by sampling, and this parameter specifies the sampling ratio 
useshuffle | ml.data.use.shuffle | This parameter specifies whether or not to shuffle data before each epoch. Note: shuffling has a great impact on performance when the data size is large, so please use it carefully. The default setting is false (do not shuffle) 
translabel | ml.data.label.trans.class | For binary classification tasks, Angel requires each label to be (+1, -1). This parameter can transform input labels into required form if the input data was labeled in (0, 1). Default setting is false (closed) 
posnegratio | ml.data.posneg.ratio | Angel also supports generating mini-batch by sampling, but in a different way than Spark. Angel firstly separates the positive and negative examples of the data set, and then generates mini-batches by separately sampling from the two sets basing on the designated pos-neg ratio in each mini-batch. This parameter is used to control the pos-neg ratio. It can help dealing with unbalanced data set. The default setting is -1 (do not sample) 

Note: all parameters except `indexrange` have default values and are optional.

## 2. Model-Related Parameters
A whole model configuration will be like:
```json
"model": {
    "loadPath": "path_to_your_model_for_loading",
    "savePath": "path_to_your_model_for_saving", 
    "modeltype": "T_DOUBLE_SPARSE_LONGKEY",
    "modelsize": 148
}
```
The Json keys are demonstrated in the table below:

Json key | Corresponding configuration item | Description 
---|---|---
loadpath | angel.load.model.path | Model loading path 
savepath | angel.save.model.path | Model save path 
modeltype | ml.model.type | Model type, i.e. the type of the input layer's parameters on PS (data type and storage type). This parameter also determines the data type of non-input layers (storage type of non-input layers is dense) 
modelsize | ml.model.size | For the whole data set, there may be some null data in some dimensions, resulting in inconsistency between feature dimension and model dimension. This parameter specifies the **valid** data dimension. 

Common modeltype includes:

- T_DOUBLE_DENSE
- T_FLOAT_DENSE
- T_DOUBLE_SPARSE
- T_FLOAT_SPARSE
- T_DOUBLE_SPARSE_LONGKEY
- T_FLOAT_SPARSE_LONGKEY

## 3. Training-Related Parameters
A whole training configuration will be like:
```json
"train": {
    "epoch": 10,
    "numupdateperepoch": 10,
    "batchsize": 1024,
    "lr": 0.5,
    "decayclass": "StandardDecay",
    "decayalpha": 0.001,
    "decaybeta": 0.9
}
```
The Json keys are demonstrated in the table below:

Json key | Corresponding configuration item | Description 
---|---|---
epoch| ml.epoch.num | Number of epochs 
numupdateperepoch | ml.num.update.per.epoch | This parameter is only useful for Angel, indicating the number of times the parameters are updated in each iteration 
batchsize |ml.minibatch.size | This parameter is only useful for Angel, indicating the size of mini-batch 
lr |ml.learn.rate | Learning rate 
decayclass |ml.opt.decay.class.name| Learning rate decay class 
decayalpha |ml.opt.decay.alpha| Alpha in the learning rate decay class 
decaybeta |ml.opt.decay.beta| Beta in the learning rate decay class 
In which:

![model](http://latex.codecogs.com/png.latex?\dpi{150}lr_{epoch}=\max(\frac{lr}{\sqrt{1.0+decay*epoch}},\frac{lr}{5}))

## 4. Default Optimizer, Transfer Function, Loss Function
- Angel allows different trainable layers to use different optimizers. For instance, in a deep and wide model, the wide part will use FTRL while the deep part will use Adam.
- Similarly, Angel also allows different layers to use different transfer functions (activation functions). For example, the middle layers use ReLu/Sigmoid, and the layer connecting with LossLayer uses Identity.
- Angel supports configuring default loss function, which is designed for multi-task training. However, Angel doesn't currently support multi-task learning.

### 4.1 Default Optimizer
Since each optimizer has its default parameters, there are two ways to set the default optimizer. Take Momentum as an example:

```json
"default_optimizer": "Momentum"

"default_optimizer": {
    "type": "Momentum",
    "momentum": 0.9,
    "reg1": 0.0,
    "reg2": 0.0
}
```
- The first way: only specifying the optimizer's name while leaving the parameters as default
- The second way: specifying the opzimizer type (name) as well as related parameters by key-value. Note that the parameters are not mandatory except the optimizer type. You can ignore some parameters if you want to use the default setting

For specific parameters of the optimizers, please refer to [Optimizers on Angel](./optimizer_on_angel.md)

### 4.2 Default Transfer Function
Most of the transfer functions have no parameter, except for a few tranfer functions such as dropout. Therefore, there are also two ways to set the default transfer function:

```json
"default_transfunc": "Relu"

"default_transfunc": {
    "type": "Dropout",
    "actiontype": "train",
    "proba": 0.5
}
```
- The first way: only specifying the name of the transfer function, automatically using default parameters if the designated function has parameters
- The second way: specifying the funtion type (name) as well as related parameters by key-value. Similar to the specification of optimizer, the parameters are not mandatory. You can ignore some parameters if you want to use the default setting

Note: since the dropout transfer function is calculated differently in training and testing (prediction), users must specify in which scenario the function is used: train, inctrain or predict.

For more details about transfer functions, please refer to [Transfer Functions in Angel](./transfunc_on_angel.md)

## 5. Layers in a Network
in Angel, each deep learning algorithm is represented by an AngelGraph, in which a node represents a Layer. Nodes can be separated into three classes basing on their topological structure:

- verge: edge node, only exists in input or output layer, such as inputLayer and lossLayer
- linear: layer with only one input and one output
- join: layer with two or more input and one output

Note: although a layer can have multiple input in Angel, there is only one output at most. Output of a layer can be used as input of multiple layers, i.e., the output can be "repetitively consumed".

In Json, all layer-related parameters are stored in one list:

```json
"layers" : [
    {parameters of layer},
    {parameters of layer},
    ...
    {parameters of layer}
]
```

Although different layers have different parameters, they share some generality:

- Each layer has a name and a type:
    + name: the unique identifier of the layer, so it cannot be repeated
    + type: the type of the layer, which is actually the **class name** corresponding to the layer
- All layers except inputLayer (DenseInputLayer, SparseInputLayer, Embedding) have a "inputlayer" parameter. A join layer has multiple input, thus its input should be specified as "inputlayers":
    + inputlayer: should be specified for linear or loss layer. The value is the name of the input layer
    + inputlayers: should be specified for join layer. The value is a list of the names of input layers
- All layers except loss layer have output, but there's no need to explicitly indicate the output in Angel, as the output relationship is already specified when specifying the input relationship. However, we still have to explicitly point out the dimensions of output:
    + outputdim: the dimensions of output
- For a trainable layer, we can specify its optimizer since it has parameters. The key is "optimizer", and its optional values are consitent with those of the "default_optimizer" parameter introduced in 4.1
- Some layers, such as DenseInputLayer, SparseInputLayer and FCLayer, can have transfer functions, specified using "transfunc" key, and optional values consitent with those of the "default_transfunc" introduced in 4.2
- LossLayer has "lossfunc" item to specify the loss funcion

Following is an example of DeepFM:

```json
"layers": [
    {
      "name": "wide",
      "type": "SimpleInputLayer",
      "outputdim": 1,
      "transfunc": "identity"
    },
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
    {
      "name": "biinnersumcross",
      "type": "BiInnerSumCross",
      "inputlayer": "embedding",
      "outputdim": 1
    },
    {
      "name": "sumPooling",
      "type": "SumPooling",
      "outputdim": 1,
      "inputlayers": [
        "wide",
        "biinnersumcross",
        "fclayer"
      ]
    },
    {
      "name": "simplelosslayer",
      "type": "SimpleLossLayer",
      "lossfunc": "logloss",
      "inputlayer": "sumPooling"
    }
  ]
```

Note: In Angel, FCLayer adopts the "folding" method when specifying parameters. For more examples, please refer to the specific algorithms and [Layers in Angel](./layers_on_angel.md)