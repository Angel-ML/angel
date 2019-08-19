# Factorization Machines      
> The Factorization Machine (FM) is a matrix-based machine learning algorithm proposed by Steffen Rendle. It can predict any real-valued vector. Its main advantages include: 1) capable of highly sparse data scenarios; 2) linear computational complexity.

## 1. Introduction of Algorithm
### Factorization Model     
* Factorization Machine Model

![model](http://latex.codecogs.com/png.latex?\dpi{150}\hat{y}(x)=b+\sum_{i=1}^n{w_ix_i}+\sum_{i=1}^n\sum_{j=i+1}^n<v_i,v_j>x_ix_j)

where：![](http://latex.codecogs.com/png.latex?\dpi{100}\inline%20<v_i,v_j>) is the dot product of two k-dimensional vectors:

![dot](http://latex.codecogs.com/png.latex?\dpi{150}\inline%20<v_i,v_j>=\sum_{i=1}^kv_{i,f}\cdot%20v_{j,f})

Model parameters are：
![parameter](http://latex.codecogs.com/png.latex?\dpi{100}\inlinew_0\in%20R,w\in%20R^n,V\in%20R^{n\times%20k})
where![](http://latex.codecogs.com/png.latex?\dpi{100}\inline%20v_i)indicates that the feature i is represented by k factors, and k is the hyperparameter that determines the factorization。

### Factorization Machines as Predictors
FM can be used for a series of predictive tasks, such as：
* classification：![](http://latex.codecogs.com/png.latex?\dpi{100}\inline%20\hat{y}) can be used directly as a predictor, and the optimization criterion is to minimize the least square difference。
* regression：can use the symbol of ![](http://latex.codecogs.com/png.latex?\dpi{100}\inline%20\hat{y}) to do classification prediction，parameters are estimated by hinge function or logistic regression function at any time.。

## 2. FM on Angel
* FM algorithm model
The model of the FM algorithm consists of two parts, wide and embedding, where wide is a typical linear model. The final output is the sum of the two parts of wide and embedding.

* FM training process
    Angel implements the gradient descent method to optimize, iteratively trains the FM model, and the logic on each iteration of the worker and PS is as follows：       
    * worker：Pull the wide and embedding matrices from the PS to the local for each iteration, calculate the corresponding gradient update value, push to PS
    * PS：PS summarizes the gradient update values pushed by all workers, averages them, calculates and updates the new wide and embedding models through the optimizer.
    
* FM prediction result：
    * format：rowID,pred,prob,label
    * caption：rowID indicates the row ID of the sample, starting from 0; pred: the predicted value of the sample; prob: the probability of the sample relative to the predicted result; label: the category into which the predicted sample is classified, when the predicted result value pred is greater than 0, Label is 1, less than 0 is -1

## 3. Running and Performance
* data format
    support Libsvm and dummy two data formats, libsvm format is as follows:
    ```
    1 1:1 214:1 233:1 234:1
    ```
    
    dummy data format:
    
    ```
    1 1 214 233 234
    ```

* Parameter Description            
    * ml.epoch.num：Number of iterations
    * ml.feature.index.range:Feature index range
    * ml.model.size：Feature dimension
    * ml.data.validate.ratio：Verification set sampling rate
    * ml.data.type：Data type, "libsvm" and "dummy"
    * ml.learn.rate：learning rate
    * ml.opt.decay.class.name：Learning rate decay
    * ml.opt.decay.on.batch: Whether to decay each mini batch
    * ml.opt.decay.alpha: Learning rate decay parameter alpha
    * ml.opt.decay.beta: Learning rate decay parameter beta
    * ml.opt.decay.intervals: Learning rate decay parameter intervals
    * ml.reg.l2:l2 regularization
    * action.type：Task type，Training with "train", prediction with "predict"
    * ml.fm.field.num:the number of input data field
    * ml.fm.rank:Length of vector in embedding
    * ml.inputlayer.optimizer：Optimizer type, optional "adam", "ftrl" and "momentum"
    * ml.data.label.trans.class: Whether to convert the label, the default is "NoTrans", the options are "ZeroOneTrans" (converted to 0-1), "PosNegTrans" (converted to plus or minus 1), "AddOneTrans" (plus 1), "SubOneTrans" ( minus 1).
    * ml.data.label.trans.threshold: "ZeroOneTrans" (turned to 0-1), "PosNegTrans" (turned to plus or minus 1) These two transitions are set to a threshold, greater than the threshold of 1, the threshold defaults to 0
    * ml.data.posneg.ratio: Positive and negative sample resampling ratio, useful for situations where the positive and negative samples differ greatly (eg, 5 times or more)
  
* submit command
   The FM algorithm can be submitted by the following command or set construct a json file of compute network and run by json(see [Json description](../basic/json_conf_en.md):

### submit by parameters

```shell
$ANGEL_HOME/bin/angel-submit \
    -Dml.epoch.num=20 \
    -Dangel.app.submit.class=com.tencent.angel.ml.core.graphsubmit.GraphRunner \
    -Dml.model.class.name=com.tencent.angel.ml.classification.FactorizationMachines \
    -Dml.feature.index.range=$featureNum \
    -Dml.model.size=$featureNum \
    -Dml.data.validate.ratio=0.1 \ 
    -Dml.data.type=libsvm \
    -Dml.learn.rate=0.1 \
    -Dml.reg.l2=0.03 \
    -Daction.type=train \
    -Dml.fm.field.num=$fielNum \
    -Dml.fm.rank=8 \
    -Dml.inputlayer.optimizer=ftrl \
    -Dangel.train.data.path=$input_path \
    -Dangel.save.model.path=$model_path \
    -Dangel.log.path=$log_path \
    -Dangel.workergroup.number $workerNumber \
    -Dangel.worker.memory.gb $workerMemory  \
    -Dangel.worker.task.number $taskNumber \
    -Dangel.ps.number $PSNumber \
    -Dangel.ps.memory.gb $PSMemory \
    -Dangel.output.path.deleteonexist true \
    -Dangel.task.data.storage.level $storageLevel \
    -Dangel.task.memorystorage.max.gb $taskMemory \
    -Dangel.worker.env "LD_PRELOAD=./libopenblas.so" \
    -Dangel.job.name=fm \
    -Dml.optimizer.json.provider com.tencent.angel.ml.core.PSOptimizerProvider
```

### submit by json

json file as follows:(see [data](https://github.com/Angel-ML/angel/tree/master/data/census))

```json
{
  "data": {
    "format": "dummy",
    "indexrange": 148,
    "validateratio": 0.1,
    "numfield": 13,
    "sampleratio": 0.2
  },
  "train": {
    "epoch": 5,
    "lr": 0.8,
    "decayclass": "WarmRestarts",
    "decayalpha": 0.05
  },
  "model": {
    "modeltype": "T_FLOAT_DENSE",
    "modelsize": 148
  },
  "default_optimizer": {
    "type": "momentum",
    "reg2": 0.01
  },
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
      "outputdim": 104
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
        "biinnersumcross"
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
*submit script

```shell
runner="com.tencent.angel.ml.core.graphsubmit.GraphRunner"
modelClass="com.tencent.angel.ml.core.graphsubmit.AngelModel"

$ANGEL_HOME/bin/angel-submit \
    --angel.job.name fm \
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
    --angel.ml.conf $fm_json_path \
    --ml.optimizer.json.provider com.tencent.angel.ml.core.PSOptimizerProvider
```