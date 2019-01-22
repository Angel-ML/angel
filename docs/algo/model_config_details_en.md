## Task Types，worker，network configurations
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
train | train | Angel task type，training model
predict | predict | use model to predict
inctrain | inctrain | incremental training of existing models
ml.matrix.dot.use.parallel.executor | false | whether to use parallel in dot of dense matrix
angel.worker.thread.num | 1 | the number of threads in a worker
angel.compress.bytes | 8 | low precision compression, the size of each floating point number can be set to [1,8]

    

## Data Parameters
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.data.type | libsvm | Angel input data types，supports:libsvm，dense，dummy
ml.data.splitor | \s+ | input data separator, customizable separator
ml.data.has.label | true | whether the input data has a label, the default has a label
ml.data.label.trans.class | NoTrans | trans the label of input data ，supports；NoTrans<do not trans>,PosNegTrans(threshold)<use threshold to convert the label +1, -1，when greater than the threshold return 1, otherwise -1>,ZeroOneTrans(threshold)<use threshold to convert the label 1, 0，when greater than the threshold return 0>,AddOneTrans<all labels +1>,SubOneTrans<all labels -1>
ml.data.label.trans.threshold | 0 | the threshold of label trans ，use with PosNegTrans and ZeroOneTrans in ml.data.label.trans.class
ml.data.validate.ratio | 0.05 | proportion of data used for validation (no validation when set to 0)
ml.feature.index.range | -1 | input data dimension, because the feature hash, can not fill the entire hash space, there are a lot of gaps, the configuration is the size of the hash space. The maximum featureID value +1, when the -1 is selected, the feature dimension can be mapped to (0, Long.max)
ml.block.size | 1000000 | The size of each block after dividing the matrix, the number of rows * the number of columns <= block.size, the purpose is to make the matrix partition evenly
ml.data.use.shuffle | false | whether to shuffle the data
ml.data.posneg.ratio | -1 | positive and negative sample sampling ratio, -1 means off sampling function, normal value is positive real number (0~1), useful for positive and negative samples with large difference (such as 5 times or more)

### Data Types
    
name | Description
---------------- | ---------------
libsvm | each line of text represents a sample, and the format of each sample is "y index1:value1 index2:value2 index3:value3 ...". Where: index is the ID of the feature, and value is the corresponding feature value; y of the training data is the class of the sample, and two values of 1, 1 can be taken; y of the predicted data is the ID value of the sample. For example, the text of a sample [2.0, 3.1, 0.0, 0.0, -1, 2.2] belonging to a positive class is represented as "1 0:2.0 1:3.1 4:-1 5:2.2", where "1" is a category," 0: 2.0" indicates that the value of the 0th feature is 2.0. Similarly, the samples belonging to the negative class [2.0, 0.0, 0.1, 0.0, 0.0, 0.0] are expressed as "-1 0: 2.0 2: 0.1"
dense | each line of text represents a sample, and the format of each sample is "y value1 value2 value3 ...". The y of the training data is the category of the sample, which can take two values of 1, 1; the y of the predicted data is the ID value of the sample. For example, the text of the sample [2.0, 3.1, 0.0, 0.0, -1, 2.2] belonging to the positive class is represented as "1 2.0 3.1 -1 2.2", where "1" is the category and "2.0" is the 0th feature. The value is 2.0. Similarly, the samples belonging to the negative class [2.0, 0.0, 0.1, 0.0, 0.0, 0.0] are expressed as "-1 2.0 0.1"
dummy | each line of text represents a sample, and each sample has the format "y index1 index2 index3 ...". Where: the ID of the index feature; the y of the training data is the category of the sample, which can take two values of 1, 1; the y of the predicted data is the ID value of the sample. For example, the text of a sample [2.0, 3.1, 0.0, 0.0, -1, 2.2] belonging to a positive class is represented as "1 0 1 4 5", where "1" is a category and "0 1 4 5" represents a feature vector. The values of the 0th, 1st, 4th, and 5th dimensions are not 0. Similarly, the samples belonging to the negative class [2.0, 0.0, 0.1, 0.0, 0.0, 0.0] are represented as "-1 0 2"



## Model Parameters
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.model.class.name | "" | the name of Angel model
ml.model.size | -1 | the size of model，when -1 is selected, the range is (0, +long.max)
ml.model.type | RowType.T_FLOAT_DENSE.toString | Angel model type，suports 28 kinds
ml.model.is.classification | true | whether the model belongs to the classification model
ml.epoch.num | 30 | Number of iterations
ml.batch.sample.ratio | 1.0 | Indicates the percentage of each batch as a percentage of the overall data
ml.learn.rate | 0.5 | learning rate
ml.num.update.per.epoch | 10 | the update number of parameter in one epoch
ml.opt.decay.class.name | StandardDecay | the name of decay class，optional：StandardDecay<Use with alpha>，WarmRestarts<Use with alpha>，CorrectionDecay<Use with alpha and beta>，ConstantLearningRate<constant decay>
ml.opt.decay.on.batch | false | whether to decay in batches
ml.opt.decay.intervals | 100 | the intervals of decay
ml.opt.decay.alpha | 0.001 | alpha in decay
ml.opt.decay.beta | 0.001 | beta in decay

### Model Types
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
RowType.T_DOUBLE_DENSE | 0 | Indicates that the type of model data row is a dense Double type with an index value in the Int range
RowType.T_DOUBLE_DENSE_COMPONENT | 1 | Indicates that the type of the model data row is a combined dense Double type with an index value in the Int range
RowType.T_DOUBLE_DENSE_LONGKEY_COMPONENT | 2 | Indicates that the type of the model data row is a combined dense Double type with an index value in the Long range.
RowType.T_DOUBLE_SPARSE | 3 | Indicates that the type of the model data row is a sparse Double type with an index value in the Int range
RowType.T_DOUBLE_SPARSE_COMPONENT | 4 | Indicates that the type of the model data row is the combined sparse Double type with the index value in the Int range.
RowType.T_DOUBLE_SPARSE_LONGKEY | 5 | Indicates that the type of the model data row is a sparse Double type with an index value in the Long range.
RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT | 6 | Indicates that the type of the model data row is the combined sparse Double type with the index value in the Int range.
RowType.T_FLOAT_DENSE | 7 | Indicates that the type of the model data row is a dense Float type with an index value in the Int range
RowType.T_FLOAT_DENSE_COMPONENT | 8 | Indicates that the type of the model data row is a combined dense Float type with an index value in the Int range.
RowType.T_FLOAT_DENSE_LONGKEY_COMPONENT | 9 | Indicates that the type of the model data row is a combined dense Float type with an index value in the Long range.
RowType.T_FLOAT_SPARSE | 10 | Indicates that the type of model data row is a sparse Float type with an index value in the Int range
RowType.T_FLOAT_SPARSE_COMPONENT | 11 | Indicates that the type of model data row is a combined sparse Float type with an index value in the Int range
RowType.T_FLOAT_SPARSE_LONGKEY | 12 | Indicates that the type of the model data row is a sparse Float type with an index value in the Long range
RowType.T_FLOAT_SPARSE_LONGKEY_COMPONENT | 13 | Indicates that the type of the model data row is a combined sparse Float type with an index value in the Long range
RowType.T_LONG_DENSE | 14 | Indicates that the type of the model data row is a dense Long type with an index value in the Int range
RowType.T_LONG_DENSE_COMPONENT | 15 | Indicates that the type of the model data row is the combined dense Long type with the index value in the Int range
RowType.T_LONG_DENSE_LONGKEY_COMPONENT | 16 | Indicates that the type of the model data row is the combined dense Long type with the index value in the Long range
RowType.T_LONG_SPARSE | 17 | Indicates that the type of the model data row is a sparse Long type with an index value in the Long range
RowType.T_LONG_SPARSE_COMPONENT | 18 | Indicates that the type of the model data row is the combined sparse Long type of the index value in the Int range
RowType.T_LONG_SPARSE_LONGKEY | 19 | Indicates that the type of the model data row is a sparse Long type with an index value in the Long range
RowType.T_LONG_SPARSE_LONGKEY_COMPONENT | 20 | Indicates that the type of the model data row is the combined sparse Long type with the index value in the Long range
RowType.T_INT_DENSE | 21 | Indicates that the type of model data row is a dense Int type with an index value in the Int range
RowType.T_INT_DENSE_COMPONENT | 22 | Indicates that the type of the model data row is the combined dense Int type with the index value in the Int range
RowType.T_INT_DENSE_LONGKEY_COMPONENT | 23 | Indicates that the type of the model data row is a combined dense Int type with an index value in the Long range
RowType.T_INT_SPARSE | 24 | Indicates that the type of the model data row is a sparse Int type with an index value in the Int range
RowType.T_INT_SPARSE_COMPONENT | 25 | Indicates that the type of the model data row is a sparse Int type with an index value in the Int range
RowType.T_INT_SPARSE_LONGKEY | 26 | Indicates that the type of the model data row is a sparse Int type with an index value in the Long range
RowType.T_INT_SPARSE_LONGKEY_COMPONENT | 27 | Indicates that the type of the model data row is the combined sparse Int type with the index value in the Long range



## Optimizer Configuration
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.fclayer.optimizer | Momentum | full connection layer optimizer, optional optimizer: Momentum，AdaDelta，AdaGrad，Adam，FTRL
ml.embedding.optimizer | Momentum | embedding layer optimizer，optional optimizer: Momentum，AdaDelta，AdaGrad，Adam，FTRL
ml.inputlayer.optimizer | Momentum |input layer optimizer, optional optimizer: Momentum，AdaDelta，AdaGrad，Adam，FTRL
ml.fclayer.matrix.output.format | classOf[RowIdColIdValueTextRowFormat].getCanonicalName | the output format of full connection layer
ml.embedding.matrix.output.format | classOf[TextColumnFormat].getCanonicalName | the output format of embedding layer
ml.simpleinputlayer.matrix.output.format | classOf[ColIdValueTextRowFormat].getCanonicalName | the output format of simpleinput layer
ml.reg.l2 | 0.0 | coefficient of the L2 penalty
ml.reg.l1 | 0.0 | coefficient of the L1 penalty

### Momentum

Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.opt.momentum.momentum | 0.9 | momentum

### AdaDelta

Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.opt.adadelta.alpha | 0.9 | alpha
ml.opt.adadelta.beta | 0.9 | beta

### AdaGrad

Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.opt.adagrad.beta | 0.9 | beta

### Adam

Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.opt.adam.gamma | 0.99 | gamma
ml.opt.adam.beta | 0.9 | beta

### FTRL

Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.opt.ftrl.alpha | 0.1 | alpha
ml.opt.ftrl.beta | 1.0 | beta



## Layers, models Parameter Configuration

### Embedding Parameter Configuration
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.fm.field.num | -1 | feature dimension, -1 for all features, and the range can be mapped to (0, +long.max)
ml.fm.rank | 8 | the length of vector in embedding

### (MLP) Layer Parameter Configuration
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.num.class | 2 | the number of classification

### (MLR) Layer Parameter Configuration

Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.mlr.rank | 5 | the number of fields

### RobustRegression Parameter Configuration

Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.robustregression.loss.delta | 1.0 | residual difference section point

### Kmeans Parameter Configuration

Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.kmeans.center.num | 5 | the number of clusters
ml.kmeans.c | 0.1 | learning rate

### GBDT Parameter Configuration

Property Name | Default | Meaning
---------------- | --------------- | ---------------
ml.gbdt.task.type | classification | task type，Optional：classification, regression
ml.gbdt.class.num | 2 | the number of the classification
ml.gbdt.tree.num | 10 | the number of trees
ml.gbdt.tree.depth | 5 | maximum tree depth
ml.gbdt.max.node.num | (none) | the maximum number of nodes
ml.gbdt.split.num | 5 | maximum size of grad/hess histograms for each feature
ml.gbdt.sample.ratio | 1 | proportion of features selected for training; default is 1
ml.gbdt.min.child.weight | 0.01 | the minimum child weight
ml.gbdt.reg.alpha | 0 | L1 regular
ml.gbdt.reg.lambda | 1.0 | L2 regular
ml.gbdt.thread.num | 20 | the number of threads
ml.gbdt.batch.size | 10000 | the size of a batch
ml.gbdt.server.split | false | if true, use two-stage tree splitting; default is false
ml.gbdt.cate.feat | none | categorical features，with the format of "feature id : feature rang" ("0:2,1:3" for instance). "none" 



## Evaluation
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
train.loss | (none) | the loss of training data
validate.loss | (none) | the loss of validating data
log.likelihood | (none) | log likelihood
train.error | (none) | the error of training data
validate.error | (none) | the error of validating data

