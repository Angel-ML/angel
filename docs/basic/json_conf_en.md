# Json Configuration File Description

Due to the large number of deep learning parameters, it is cumbersome to specify parameters directly by script. From Angel 2.0, it is supported to specify model parameters with Json files (System parameters are still configured by script). Currently, Json files have two purposes:
- Provide parameters for existing algorithms, including:
    + DeepFM
    + PNN
    + NFM
    + Deep and Wide
- Similar to Caffe, you can customize your new network with Json


The parameters that can be configured with Json files are as follows:

- Data-related parameters

- Model parameters

- Training related parameters

- Default optimizer, transfer function, loss function

- Layers in the Network

Here's an introduction.


## 1. Data-related Parameters

A complete data configuration is as follows:

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

Json key | configuration item | description
---|---|---
format | ml.data.type | Input data format,including dense, dummy and libsvm, the default one is dummy
indexrange | ml.feature.index.range | Feature dimension
numfield | ml.fm.field.num | Number of fields in the input data. Although feature intersections can generate very high dimensional data, for some data sets, the number of each sample field is constant.
validateratio | ml.data.validate.ratio | Angel will divide the input data into a training set and a validation set. This parameter is used to specify the scale of the validation set. Note: All validation sets are evenly stored in memory. OOM may occurs when the amount of data is particularly large.
sampleratio | ml.batch.sample.ratio | The sampling rate for spark to sample mini-batches.
useshuffle | ml.data.use.shuffle | Whether to shuffle data before training each epoch, the default set is false. Note: large dataset with shuffle training may cause poor performance.
translabel | ml.data.label.trans.class | For two classes classification task, Angel requires the label format as (-1, 1),set it to true if your label format is (0, 1). The default set is false.
posnegratio | ml.data.posneg.ratio | Angel supports sampling mini-batches, but in a different way from Spark. Angel firstly separates the positive and negative examples of the data set, and then separately generates mini-batch from positive and negative samples in proportion. Parameter posnegratio is the ratio of positive and negative samples in a mini-batch. This parameter is helpful for the training of unbalanced data sets. posnegratio defaults to -1, indicating no sampling.

Note: In addition to indexrange, all other parameters have default values, which means they are optional.

## 2. Model Parameters

A complete model configuration is as follows:
```json
"model": {
    "loadPath": "path_to_your_model_for_loading",
    "savePath": "path_to_your_model_for_saving", 
    "modeltype": "T_DOUBLE_SPARSE_LONGKEY",
    "modelsize": 148
}
```

Json key | configuration item | description
---|---|---
loadpath | angel.load.model.path | Model loading path
savepath | angel.save.model.path | Model save path
modeltype | ml.model.type | Model type, refers to the type of input layer parameters on the PS (data and storage), and it also determines the data type of the non-input layer (the storage type of the non-input layer is dense)
modelsize | ml.model.size | The effective data dimension. For the entire dataset, there may be no data in some dimensions, and the feature dimension is inconsistent with the model dimension

Commonly used modeltypes are:
- T_DOUBLE_DENSE
- T_FLOAT_DENSE
- T_DOUBLE_SPARSE
- T_FLOAT_SPARSE
- T_DOUBLE_SPARSE_LONGKEY
- T_FLOAT_SPARSE_LONGKEY

## 3. Training Parameters
A complete training configuration is as follows:
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

Json key | configuration item | description
---|---|---
epoch| ml.epoch.num | Num of epochs
numupdateperepoch | ml.num.update.per.epoch | Number of times the parameter data is updated in each iteration. Only useful for Angel
batchsize |ml.minibatch.size | Number of samples in each mini-batch
lr |ml.learn.rate | Learning rate
decayclass |ml.opt.decay.class.name| Learning rate decay method
decayalpha |ml.opt.decay.alpha| Learning rate decay parameter alpha
decayalpha |ml.opt.decay.beta| Learning rate decay parameterbeta

![model](http://latex.codecogs.com/png.latex?\dpi{150}lr_{epoch}=\max(\frac{lr}{\sqrt{1.0+decay*epoch}},\frac{lr}{5}))

## 4. Default optimizer, transfer function, loss function
- Angel allows different trainable layers to use different optimizers. For example, in the deep and wide model, the wide part uses FTRL and the deep part uses Adam.
- Similarly, different layers are allowed to use different transfer functions (activation functions). Usually, the intermediate layers use Relu or Sigmoid function, and the layer connected to the LossLayer uses Identity function.
- Angel can set the default loss function, which is designed for multitasking training. Multitask training is not supported yet, please ignore it.

### 4.1 Default optimizer
Since each optimizer has default parameters, there are two ways to set the default optimizer. Take Momentum as an example:
```json
"default_optimizer": "Momentum"

"default_optimizer": {
    "type": "Momentum",
    "momentum": 0.9,
    "reg1": 0.0,
    "reg2": 0.0
}
```

- Specify only the optimizer name and use the default optimizer parameters 
- Use the key-value method to specify the type (name) and related parameters of the optimizer. It is worth noting that the parameters of the optimizer are not mandatory except for the name. If you want to use the default value, just ignore it.

For specific parameters of the optimizer, please refer to [Optimizer in Angel] (./optimizer_on_angel.md)

### 4.2 Default transfer function
Most transfer functions have no parameters, only a few transfer functions do, such as dropout. So there are two ways to specify the default transfer function:
```json
"default_transfunc": "Relu"

"default_transfunc": {
    "type": "Dropout",
    "actiontype": "train",
    "proba": 0.5
}
```

- Only specify the name of the transfer function, if the transfer function has parameters, use the default parameters
- Similar to the parameter specification of the optimizer, use the key-value method to specify the type (name) of the transfer function and related parameters. The parameters of the transfer function are not required, if you want to use the default value, just ignore it  directly.

Note: Since the dropout transfer function is not calculated the same way in training and testing (predictive), use actiontype to indicate the task type: train | inctrain | predict.

For more details on the transfer function, please refer to [Transfer Function in Angel] (./transfunc_on_angel.md)

## 5. Layers in the network

Each deep learning algorithm in Angel can be represented as an AngelGraph, and the nodes in AngelGraph are Layers. Layers can be divided into three categories:
- verge: edge node, only in the input or output layer, such as input layer and loss layer
- linear: layer with one input and one output
- join: layer with two or more inputs, one output

Note: Layers in Angel can have multiple inputs, but only one output is allowed. A layer's output can be used as input for multiple layers, which means the output can be "repeatedly consumed".

In the Json configuration file, all layer-related parameters are placed in a list, as follows:

```json
"layers" : [
    {parameters of layer},
    {parameters of layer},
    ...
    {parameters of layer}
]
```

Although different layers have different parameters, they have some commonalities:
- Each layer has a name (name) and a type (type):
    + name: is the unique identifier of the layer, so it cannot be repeated
    + type: is the type of the layer, which is actually the `class name of the Layer'.
- In addition to the input layer (DenseInputLayer, SparseInputLayer, Embedding), the other layers have the "inputlayer" parameter. For the join layer, it has multiple inputs, so its input is specified with "inputlayers":
    + inputlayer: For the linear or loss layer to be specified, the inputlayer value is the name of the input layer
    + inputlayers: For the join layer to be specified, represented by a list whose value is the name of the input layer
- All layers have output except the Loss layer, but explicitly pointing out the outputs is not needed. The dimensions of the output should be specified:
    + outputdim: the dimensions of the output
- For trainable layer, "optimizer" can be specified, otherwise the default optimizers are used
- For some layers, such as DenseInputLayer, SparseInputLayer, FCLayer, "transfunc" can be specified
- For the loss layer, "lossfunc" can be specified

Here is an example of DeepFM:

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

Note: Angel supports "folding" method when specifying sequential FC layers. For more examples, please refer to the specific algorithm and [layer in Angel] (./layers_on_angel.md)
