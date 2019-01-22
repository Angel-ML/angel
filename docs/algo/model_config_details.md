## 任务类型，worker，network配置
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
train | train | Angel 任务类型，表示模型训练
predict | predict | 使用模型进行预测
inctrain | inctrain | 对已有模型进行增量训练
ml.matrix.dot.use.parallel.executor | false | 稠密矩阵Dot运算是否使用并行
angel.worker.thread.num | 1 | 一个worker使用的线程数
angel.compress.bytes | 8 | 低精度压缩，每个浮点数的大小，可设为[1,8]

    

## 数据参数
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.data.type | libsvm | Angel 模型输入数据格式，支持libsvm，dense，dummy；默认值为libsvm
ml.data.splitor | \s+ | 输入数据分隔符，可定制分隔符
ml.data.has.label | true | 输入数据是否有标签，默认有标签
ml.data.label.trans.class | NoTrans | 输入数据标签转换，支持NoTrans<不转换>,PosNegTrans(threshold)<利用阈值将标签转换成+1，-1，即大于阈值为1，小于阈值为-1>,ZeroOneTrans(threshold)<利用阈值将标签转换成0, 1，即大于阈值为1，小于阈值为0>,AddOneTrans<所有标签+1>,SubOneTrans<所有标签-1>
ml.data.label.trans.threshold | 0 | 输入数据标签转换阈值，与ml.data.label.trans.class中的PosNegTrans，ZeroOneTrans搭配使用
ml.data.validate.ratio | 0.05 | 验证集采样率
ml.feature.index.range | -1 | 输入数据维度，由于特征hash时, 不能充满整个hash空间, 其中有大量空隙, 该项配置就是hash空间的大小. 最大的featureID值+1，当选择-1时表示特征维度可以映射到(0, long.max) 
ml.block.size | 1000000 | 划分矩阵后每个块的的大小，行数*列数<=block.size，目的是使得矩阵划分的均匀
ml.data.use.shuffle | false | 数据是否使用随机打乱
ml.data.posneg.ratio | -1 | 正负样本采样比例，-1表示关闭采样功能, 正常值为正实数（0~1），对于正负样本相差较大的情况有用(如5倍以上)

### 数据格式
    
名称 | 说明
---------------- | ---------------
libsvm | 每行文本表示一个样本，每个样本的格式为"y index1:value1 index2:value2 index3:value3 ..."。其中：index为特征的ID,value为对应的特征值；训练数据的y为样本的类别，可以取1、-1两个值；预测数据的y为样本的ID值。比如，属于正类的样本[2.0, 3.1, 0.0, 0.0, -1, 2.2]的文本表示为“1 0:2.0 1:3.1 4:-1 5:2.2”，其中“1”为类别，"0:2.0"表示第0个特征的值为2.0。同理，属于负类的样本[2.0, 0.0, 0.1, 0.0, 0.0, 0.0]被表示为“-1 0:2.0 2：0.1”
dense | 每行文本表示一个样本，每个样本的格式为"y value1 value2 value3 ..."。训练数据的y为样本的类别，可以取1、-1两个值；预测数据的y为样本的ID值。比如，属于正类的样本[2.0, 3.1, 0.0, 0.0, -1, 2.2]的文本表示为“1 2.0 3.1 -1 2.2”，其中“1”为类别，"2.0"表示第0个特征的值为2.0。同理，属于负类的样本[2.0, 0.0, 0.1, 0.0, 0.0, 0.0]被表示为“-1 2.0 0.1”
dummy | 每行文本表示一个样本，每个样本的格式为"y index1 index2 index3 ..."。其中：index特征的ID；训练数据的y为样本的类别，可以取1、-1两个值；预测数据的y为样本的ID值。比如，属于正类的样本[2.0, 3.1, 0.0, 0.0, -1, 2.2]的文本表示为“1 0 1 4 5”，其中“1”为类别，“0 1 4 5”表示特征向量的第0、1、4、5个维度的值不为0。同理，属于负类的样本[2.0, 0.0, 0.1, 0.0, 0.0, 0.0]被表示为“-1 0 2”



## 模型参数
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.model.class.name | "" | Angel模型类名称
ml.model.size | -1 | 模型的大小，当选择-1时范围在(0, +long.max) 
ml.model.type | RowType.T_FLOAT_DENSE.toString | Angel模型类型，支持28种类型
ml.model.is.classification | true | 模型是否属于分类模型
ml.epoch.num | 30 | 迭代次数
ml.batch.sample.ratio | 1.0 | 表示每个batch占整体数据的百分比
ml.learn.rate | 0.5 | 初始学习率
ml.num.update.per.epoch | 10 | 一个epoch中更新参数的次数
ml.opt.decay.class.name | StandardDecay | 衰减类名称，可选项：StandardDecay<标准衰减，与alpha一起使用>，WarmRestarts<热启动衰减，与alpha一起使用>，CorrectionDecay<更正衰减，与alpha和beta一起使用>，ConstantLearningRate<常数衰减>
ml.opt.decay.on.batch | false | 是否在批量上进行衰减
ml.opt.decay.intervals | 100 | 衰减间隔
ml.opt.decay.alpha | 0.001 | 衰减系数alpha
ml.opt.decay.beta | 0.001 | 衰减系数beta

### 模型类型
    
名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
RowType.T_DOUBLE_DENSE | 0 | 表示模型数据行的类型是索引值在Int范围的稠密Double类型
RowType.T_DOUBLE_DENSE_COMPONENT | 1 | 表示模型数据行的类型是索引值在Int范围的组合稠密Double类型
RowType.T_DOUBLE_DENSE_LONGKEY_COMPONENT | 2 | 表示模型数据行的类型是索引值在Long范围的组合稠密Double类型
RowType.T_DOUBLE_SPARSE | 3 | 表示模型数据行的类型是索引值在Int范围的稀疏Double类型
RowType.T_DOUBLE_SPARSE_COMPONENT | 4 | 表示模型数据行的类型是索引值在Int范围的组合稀疏Double类型
RowType.T_DOUBLE_SPARSE_LONGKEY | 5 | 表示模型数据行的类型是索引值在Long范围的稀疏Double类型
RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT | 6 | 表示模型数据行的类型是索引值在Int范围的组合稀疏Double类型
RowType.T_FLOAT_DENSE | 7 | 表示模型数据行的类型是索引值在Int范围的稠密Float类型
RowType.T_FLOAT_DENSE_COMPONENT | 8 | 表示模型数据行的类型是索引值在Int范围的组合稠密Float类型
RowType.T_FLOAT_DENSE_LONGKEY_COMPONENT | 9 | 表示模型数据行的类型是索引值在Long范围的组合稠密Float类型
RowType.T_FLOAT_SPARSE | 10 | 表示模型数据行的类型是索引值在Int范围的稀疏Float类型
RowType.T_FLOAT_SPARSE_COMPONENT | 11 | 表示模型数据行的类型是索引值在Int范围的组合稀疏Float类型
RowType.T_FLOAT_SPARSE_LONGKEY | 12 | 表示模型数据行的类型是索引值在Long范围的稀疏Float类型
RowType.T_FLOAT_SPARSE_LONGKEY_COMPONENT | 13 | 表示模型数据行的类型是索引值在Long范围的组合稀疏Float类型
RowType.T_LONG_DENSE | 14 | 表示模型数据行的类型是索引值在Int范围的稠密Long类型
RowType.T_LONG_DENSE_COMPONENT | 15 | 表示模型数据行的类型是索引值在Int范围的组合稠密Long类型
RowType.T_LONG_DENSE_LONGKEY_COMPONENT | 16 | 表示模型数据行的类型是索引值在Long范围的组合稠密Long类型
RowType.T_LONG_SPARSE | 17 | 表示模型数据行的类型是索引值在Long范围的稀疏Long类型
RowType.T_LONG_SPARSE_COMPONENT | 18 | 表示模型数据行的类型是索引值在Int范围的组合稀疏Long类型
RowType.T_LONG_SPARSE_LONGKEY | 19 | 表示模型数据行的类型是索引值在Long范围的稀疏Long类型
RowType.T_LONG_SPARSE_LONGKEY_COMPONENT | 20 | 表示模型数据行的类型是索引值在Long范围的组合稀疏Long类型
RowType.T_INT_DENSE | 21 | 表示模型数据行的类型是索引值在Int范围的稠密Int类型
RowType.T_INT_DENSE_COMPONENT | 22 | 表示模型数据行的类型是索引值在Int范围的组合稠密Int类型
RowType.T_INT_DENSE_LONGKEY_COMPONENT | 23 | 表示模型数据行的类型是索引值在Long范围的组合稠密Int类型
RowType.T_INT_SPARSE | 24 | 表示模型数据行的类型是索引值在Int范围的稀疏Int类型
RowType.T_INT_SPARSE_COMPONENT | 25 | 表示模型数据行的类型是索引值在Int范围的稀疏Int类型
RowType.T_INT_SPARSE_LONGKEY | 26 | 表示模型数据行的类型是索引值在Long范围的稀疏Int类型
RowType.T_INT_SPARSE_LONGKEY_COMPONENT | 27 | 表示模型数据行的类型是索引值在Long范围的组合稀疏Int类型



## 优化器配置
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.fclayer.optimizer | Momentum | 全连接层优化器，可选优化器：Momentum，AdaDelta，AdaGrad，Adam，FTRL
ml.embedding.optimizer | Momentum | embedding层优化器，可选优化器：Momentum，AdaDelta，AdaGrad，Adam，FTRL
ml.inputlayer.optimizer | Momentum | 输入层优化器，可选优化器：Momentum，AdaDelta，AdaGrad，Adam，FTRL
ml.fclayer.matrix.output.format | classOf[RowIdColIdValueTextRowFormat].getCanonicalName | 全连接层输出矩阵格式
ml.embedding.matrix.output.format | classOf[TextColumnFormat].getCanonicalName | embedding层输出矩阵格式
ml.simpleinputlayer.matrix.output.format | classOf[ColIdValueTextRowFormat].getCanonicalName | 简单输入层输出矩阵格式
ml.reg.l2 | 0.0 | L2惩罚项系数
ml.reg.l1 | 0.0 | L1惩罚项系数

### Momentum

配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.opt.momentum.momentum | 0.9 | 动量系数

### AdaDelta

配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.opt.adadelta.alpha | 0.9 | alpha系数
ml.opt.adadelta.beta | 0.9 | beta系数

### AdaGrad

配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.opt.adagrad.beta | 0.9 | beta系数

### Adam

配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.opt.adam.gamma | 0.99 | gamma系数
ml.opt.adam.beta | 0.9 | beta系数

### FTRL

配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.opt.ftrl.alpha | 0.1 | alpha系数
ml.opt.ftrl.beta | 1.0 | beta系数



## 层，模型等参数配置

### Embedding 参数配置
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.fm.field.num | -1 | 输入数据特征维数，-1表示全部特征，范围可以映射到(0, +long.max)
ml.fm.rank | 8 | embedding中vector的长度

### (MLP) Layer 参数配置
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.num.class | 2 | 类别数目

### (MLR) Layer 参数配置

配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.mlr.rank | 5 | 区域个数

### RobustRegression 参数配置

配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.robustregression.loss.delta | 1.0 | 残差分段点

### Kmeans 参数配置

配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.kmeans.center.num | 5 | K值，即簇的个数
ml.kmeans.c | 0.1 | 学习速率参数

### GBDT 参数配置

配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
ml.gbdt.task.type | classification | 任务类型，可选项：classification, regression
ml.gbdt.class.num | 2 | 类别个数
ml.gbdt.tree.num | 10 | 树的数量
ml.gbdt.tree.depth | 5 | 树的最大高度
ml.gbdt.max.node.num | 无 | 最大的节点个数
ml.gbdt.split.num | 5 | 每个特征的梯度直方图的大小
ml.gbdt.sample.ratio | 1 | 特征下采样的比率
ml.gbdt.min.child.weight | 0.01 | 最小的子节点权重
ml.gbdt.reg.alpha | 0 | L1正则
ml.gbdt.reg.lambda | 1.0 | L2正则
ml.gbdt.thread.num | 20 | 线程个数
ml.gbdt.batch.size | 10000 | 批量大小
ml.gbdt.server.split | false | 两阶段分裂算法开关
ml.gbdt.cate.feat | none | 离散特征，"特征id:特征数量"的格式，以逗号分隔，例如"0:2,1:3"。设为"none"表示没有离散特征，设为"all"表示全部为离散特征。



## 评估指标
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
train.loss | 无 | 训练损失
validate.loss | 无 | 验证损失
log.likelihood | 无 | 对数似然
train.error | 无 | 训练错误
validate.error | 无 | 验证错误

