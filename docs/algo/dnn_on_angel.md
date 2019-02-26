# DNN

DNN是每个深度学习平台第一个要支持的算法. Angel作为一个深度学习平台, 并没有内置DNN的实现, 但它提供了一种方法让用户实现自已的深度学习算法. 事实上, DeepFM, PNN, NFM, Deep and Wide等算法匀可用本文所述的方法实现.

![DNN](../img/DNN.PNG)

Angel提供了用Json配置算法的功能, 同时也提供了用Json定义算法的功能. 下面以DNN的实现为例说明.

## 1. Json定义网络

### 1.1 输入层的定义
```json
{
    "name": "denseinputlayer",
    "type": "simpleinputlayer",
    "outputdim": 500,
    "transfunc": "relu",
    "optimizer": "ftrl"
},
```
这里用一个DenseInputLayer作为输入层, 输出维度为500, 用Relu作为传递函数. 为了其它层能引用它, 给输入层命名为"denseinputlayer".

需要特别指出的是, Angel允许网络的不同部分使用不同的优化器, 这里指定DenseInputLayer使用FTRL优化器(关于优化器, 请参考[Angel中的优化器](../basic/optimizer_on_angel.md))

### 1.2 FCLayer的定义
```json
{
    "name": "fclayer",
    "type": "FCLayer",
    "outputdims": [
        100,
        1
    ],
    "transfuncs": [
        "relu",
        "identity"
    ],
    "inputlayer": "denseinputlayer"
},
```

有两个FCLayer:
- 第一个FCLayer
    - 名称为"fclayer_0"
    - 输出维度为100
    - 传递函数为"Relu"
- 第二个FCLayer
    - 名称为"fclayer"
    - 输出维度为1
    - 传递函数为"identity"

从上可以看出, FCLayer使用了简约的书写方式, 请参考[Json的定义](../basic/json_conf.md). 简约方式的inputlayer为第一个FCLayer的输入, 简约方式的name为最后一个FCLayer的name. 中间的Layer命名方式是"fclayer_x", 表示FCLayer的编号, 从0开始.

另外, 简约形式的FCLayer的输入是上层"denseinputlayer"的输出.

注: FCLayer没有指定optimizer, 则它会使用默认的optimizer, 即Momentum.

### 1.3 损失层的定义
```json
{
    "name": "simplelosslayer",
    "type": "simplelosslayer",
    "lossfunc": "logloss",
    "inputlayer": "fclayer"
}
```

损失的类型是SimpleLossLayer, 损失函数为"logloss", 输入层是"fclayer"

## 2. 数据相关参数的定义
```json
  "data": {
    "format": "libsvm",
    "indexrange": 300,
    "validateratio": 0.2,
    "posnegratio": 0.1
  },
```
详细参数的意义请参考[Json的定义](../basic/json_conf.md). 这里
只列出了如下几个参数:
- format: 输入数据的格式
- indexrange: 特征维度
- validateratio: 验证集的比例
- posnegratio: 打开采样, 并设置正负样本的比例

## 3. 定义模型相关参数
```json
  "model": {
    "modeltype": "T_DOUBLE_SPARSE",
    "modelsize": 300
  },
```
详细参数的意义请参考[Json的定义](../basic/json_conf.md). 这里只列出几个参数:
- 模型类型: T_DOUBLE_SPARSE
- 模型大小: 模型的实际大小(有效特征的个数)

## 4. 定义训练参数
```json
  "train": {
    "epoch": 30,
    "numupdateperepoch": 10,
    "lr": 0.1,
    "decayclass": "WarmRestarts",
    "decaybeta": 0.001
  },
```
详细参数的意义请参考[Json的定义](../basic/json_conf.md). 这里只列出几个参数:
- epoch: 训练轮数
- lr: 学习率
- decayclass: 学习率衰减系类
- decayalpha: 学习率衰减参数
- decaybeta: 学习率衰减参数


## 5. 将所有配置放在一起
```json
{
  "data": {
    "format": "libsvm",
    "indexrange": 300,
    "validateratio": 0.2,
    "posnegratio": 0.1
  },
  "model": {
    "modeltype": "T_DOUBLE_SPARSE",
    "modelsize": 300
  },
  "train": {
    "epoch": 30,
    "lr": 0.1,
    "decayclass": "WarmRestarts",
    "decaybeta": 0.001
  },
  "default_optimizer": "momentum",
  "layers": [
    {
      "name": "simpleinputlayer",
      "type": "simpleinputlayer",
      "outputdim": 500,
      "transfunc": "relu"
    },
    {
      "name": "fclayer",
      "type": "FCLayer",
      "outputdims": [
        100,
        1
      ],
      "transfuncs": [
        "relu",
        "identity"
      ],
      "inputlayer": "denseinputlayer"
    },
    {
      "name": "simplelosslayer",
      "type": "simplelosslayer",
      "lossfunc": "logloss",
      "inputlayer": "fclayer"
    }
  ]
}

```

## 6. 提效脚本 
```shell
runner="com.tencent.angel.ml.core.graphsubmit.GraphRunner"
modelClass="com.tencent.angel.ml.core.graphsubmit.GraphModel"

$ANGEL_HOME/bin/angel-submit \
    --angel.job.name DeepFM \
    --action.type train \
    --angel.app.submit.class $runner \
    --ml.model.class.name $modelClass \
    --angel.train.data.path $input_path \
    --angel.workergroup.number $workerNumber \
    --angel.worker.memory.gb $workerMemory  \
    --angel.ps.number $PSNumber \
    --angel.ps.memory.gb $PSMemory \  
    --angel.task.data.storage.level $storageLevel \
    --angel.task.memorystorage.max.gb $taskMemory
```

注: 与其它算法的不同在于模型类使用的是:
"com.tencent.angel.ml.core.graphsubmit.GraphModel"

所有用Json定义的算法匀用这个模型类.
