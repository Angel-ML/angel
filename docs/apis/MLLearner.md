# MLLearner
模型训练接口定义。
## train
- 定义：```def train(train: DataBlock[LabeledData], vali: DataBlock[LabeledData])```


- 功能描述：使用算法对训练数据进行训练，得到模型


- 参数：train: DataBlock[LabeledData] 训练数据集； vali: DataBlock[LabeledData] 验证数据集


- 返回值：训练结果