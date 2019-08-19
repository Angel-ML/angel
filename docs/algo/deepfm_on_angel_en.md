# DeepFM

## 1. Introduction of Algorithm
The DeepFM algorithm adds a depth layer to the FM (Factorization Machine). Compared with the PNN and NFM algorithms, it preserves the second-order implicit feature intersection of FM and uses the deep network to obtain high-order feature intersections. The structure is as follows:

![DeepFM](../img/DeepFM.PNG)

### 1.1 Description of the Embedding and BiInnerSumCross layer
Different from the traditional FM implementation, the combination of Embedding and BiInnerSumCross is used to implement the second-order implicit crossover. The expression of the traditional FM quadratic cross term is as follows::

![model](http://latex.codecogs.com/png.latex?\dpi{150}\sum_i\sum_{j=i+1}\bold{v}_i^T\bold{v}_jx_ix_j=\frac{1}{2}\(\sum_i\sum_j(x_i\bold{v}_i)^T(x_j\bold{v}_j)-\sum_i(x_i\bold{v}_i)^T(x_i\bold{v}_i)\))

In implementation, it is stored by Embedding![](http://latex.codecogs.com/png.latex?\bold{v}_i), After calling Embedding's 'calOutput', computing![](http://latex.codecogs.com/png.latex?x_i\bold{v}_i) and output result together. So the Embedding output of a sample is

![model](http://latex.codecogs.com/png.latex?\dpi{150}(x_1\bold{v}_1,x_2\bold{v}_2,x_3\bold{v}_3,\cdots,x_k\bold{v}_k)=(\bold{u}_1,\bold{u}_2,\bold{u}_3,\cdots,\bold{u}_k))

The result of the original quadratic term can be re-expressed as::

![model](http://latex.codecogs.com/png.latex?\dpi{150}\sum_i\sum_{j=i+1}\bold{v}_i^T\bold{v}_jx_ix_j=\frac{1}{2}\((\sum_i\bold{u}_i)^T(\sum_j\bold{u}_j)-\sum_i\bold{u}_i^T\bold{u}_i\))

The above is BiInnerSumCross's forward calculation method, which is implemented by Scala code:
```scala
val sumVector = VFactory.denseDoubleVector(mat.getSubDim)

(0 until batchSize).foreach { row =>
    val partitions = mat.getRow(row).getPartitions
    partitions.foreach { vectorOuter =>
    data(row) -= vectorOuter.dot(vectorOuter)
    sumVector.iadd(vectorOuter)
    }
    data(row) += sumVector.dot(sumVector)
    data(row) /= 2
    sumVector.clear()
}
```

### 1.2 Description of other layers
- SimpleInputLayer: Sparse data input layer, specially optimized for sparse high-dimensional data, essentially a FClayer
- FCLayer: The most common layer in DNN, linear transformation followed by transfer function
- SumPooling: Adding multiple input data as element-wise, requiring inputs have the same shape
- SimpleLossLayer: Loss layer, you can specify different loss functions

### 1.3 Building Network
```scala
  override def buildNetwork(): Unit = {
    ensureJsonAst()

    val wide = new SimpleInputLayer("input", 1, new Identity(),
      JsonUtils.getOptimizerByLayerType(jsonAst, "SparseInputLayer")
    )

    val embeddingParams = JsonUtils.getLayerParamsByLayerType(jsonAst, "Embedding")
      .asInstanceOf[EmbeddingParams]
    val embedding = new Embedding("embedding", embeddingParams.outputDim,
      embeddingParams.numFactors, embeddingParams.optimizer.build()
    )

    val innerSumCross = new BiInnerSumCross("innerSumPooling", embedding)

    val mlpLayer = JsonUtils.getFCLayer(jsonAst, embedding)

    val join = new SumPooling("sumPooling", 1, Array[Layer](wide, innerSumCross, mlpLayer))

    new SimpleLossLayer("simpleLossLayer", join, lossFunc)
  }
```

## 2.  Running
### 2.1 Explanation of Json configuration File

There are many parameters of DeepFM, which need to be specified by Json configuration file (for a complete description of Json configuration file, please refer to[Json explanation](../basic/json_conf_en.md)), A typical example is:(see [data](https://github.com/Angel-ML/angel/tree/master/data/census))

```json
{
  "data": {
    "format": "dummy",
    "indexrange": 148,
    "numfield": 13,
    "validateratio": 0.1,
    "sampleratio": 0.2
  },
  "model": {
    "modeltype": "T_DOUBLE_SPARSE_LONGKEY",
    "modelsize": 148
  },
  "train": {
    "epoch": 10,
    "numupdateperepoch": 10,
    "lr": 0.5,
    "decayclass": "StandardDecay",
    "decaybeta": 0.01
  },
  "default_optimizer": "Momentum",
  "layers": [
    {
      "name": "wide",
      "type": "simpleinputlayer",
      "outputdim": 1,
      "transfunc": "identity"
    },
    {
      "name": "embedding",
      "type": "embedding",
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
      "type": "simplelosslayer",
      "lossfunc": "logloss",
      "inputlayer": "sumPooling"
    }
  ]
}
```

### 2.2 Explanation of submit scripts

```shell
runner="com.tencent.angel.ml.core.graphsubmit.GraphRunner"
modelClass="com.tencent.angel.ml.core.graphsubmit.AngelModel"

$ANGEL_HOME/bin/angel-submit \
    --angel.job.name DeepFM \
    --action.type train \
    --angel.app.submit.class $runner \
    --ml.model.class.name $modelClass \
    --angel.train.data.path $input_path \
    --angel.save.model.path $model_path \
    --angel.log.path $log_path \
    --angel.workergroup.number $workerNumber \
    --angel.worker.memory.gb $workerMemory  \
    --angel.worker.task.number $taskNumber \
    --angel.ps.number $PSNumber \
    --angel.ps.memory.gb $PSMemory \
    --angel.output.path.deleteonexist true \
    --angel.task.data.storage.level $storageLevel \
    --angel.task.memorystorage.max.gb $taskMemory \
    --angel.worker.env "LD_PRELOAD=./libopenblas.so" \
    --angel.ml.conf $deepfm_json_path \
    --ml.optimizer.json.provider com.tencent.angel.ml.core.PSOptimizerProvider
```

For the deep learning model, its data, training and network configuration should be specified with the Json file first.
Resources such as: worker,ps depend on detail dataset.

