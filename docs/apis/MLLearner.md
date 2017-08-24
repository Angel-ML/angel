# MLLearner

> 模型训练的核心类，理论上，Angel所有模型训练核心逻辑，都应该写在这个类中。通过这个类来实现和调用。它是Train的核心类。

## 功能

* 不断读取DataBlock，训练得到MLModel模型

## 核心接口

1. **train**
	- 定义：```def train(train: DataBlock[LabeledData], vali: DataBlock[LabeledData])：MLModel```
	- 功能描述：使用算法对训练数据进行训练，得到模型
	- 参数：train: DataBlock[LabeledData] 训练数据集； vali: DataBlock[LabeledData] 验证数据集
	- 返回值：MLModel