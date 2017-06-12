# MLRunner
算法的启动类。它定义了启动Angel app和运行task的接口，需要应用程序根据需要实现这些接口。当然，它在封装了使用 [AngelClient](AngelClient.md)的使用。 

包括：启动Angel ps，加载和存储模型，和启动task等过程的默认实现。一般情况下，应用程序直接调用它们就可以了。

## train
- 定义：``` train(conf: Configuration)```
- 功能描述：启动Angel app训练模型
- 参数：conf: Configuration Angel任务相关配置和算法配置信息
- 返回值：无

## train（default implementation）

- **定义**：```train(conf: Configuration, model: MLModel, taskClass: Class[_ <: BaseTask[_, _, _]]): Unit```

- **功能描述**：启动Angel app训练模型。该方法封装了具体的Angel ps/worker启动以及模型加载/存储过程，子类可直接引用
- **参数**
	* conf: Configuration Angel任务相关配置和算法配置信息
	* model: MLModel 算法模型信息
	* taskClass: Class[_ <: TrainTask[_, _, _]] 表示算法运行过程的Task 类
	* 返回值：无

## incTrain
* **定义**：```incTrain(conf: Configuration)```
- **功能描述**：启动Angel app 并使用增量训练的方式更新一个旧模型
- **参数**
	- conf: Configuration Angel任务相关配置和算法配置信息
- **返回值**：无

## predict
- **定义**：```predict(conf: Configuration)```


- **功能描述**：启动Angel app，计算预测结果


- **参数**：conf: Configuration Angel任务相关配置


- **返回值**：无

## predict（default implementation）
- **定义：**```predict(conf: Configuration, model: MLModel, taskClass: Class[_ <: PredictTask[_, _, _]]): Unit```


- **功能描述：**启动Angel app 并使用增量训练的方式更新一个旧模型。该方法封装了具体的Angel ps/worker启动以及模型加载/存储过程，子类可直接引用


- **参数：**conf: Configuration Angel任务相关配置


- **返回值：**无