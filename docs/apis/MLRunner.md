# MLRunner

> Angel算法的启动入口类。它定义了启动Angel任务的标准流程，封装了对 [AngelClient](AngelClient.md)的使用。 

## 功能

* 通过调用[AngelClient](./AngelClient.md)，启动Angel ps，加载和存储模型，完成Task等一系列动作，完成机器学习任务的默认实现
* 一般情况下，应用程序直接调用它的默认实现就可以了，不必重写。

## 核心方法

1. **train**
	- 定义：``` train(conf: Configuration)```
	- 功能描述：启动Angel app训练模型
	- 参数：conf: Configuration Angel任务相关配置和算法配置信息
	- 返回值：无

2. **train（default implementation）**

	- **定义**： ```train(conf: Configuration, model: MLModel, taskClass: Class[_ <: BaseTask[_, _, _]]): Unit```

	- **功能描述**：启动Angel app训练模型。该方法封装了具体的Angel ps/worker启动以及模型加载/存储过程，子类可直接引用

	- **参数**
		* conf: Configuration Angel任务相关配置和算法配置信息
		* model: MLModel 算法模型信息
		* taskClass: Class[_ <: TrainTask[_, _, _]] 表示算法运行过程的Task 类
		* 返回值：无

3. **incTrain**

	* **定义**：```incTrain(conf: Configuration)```
	- **功能描述**：使用增量训练的方式更新一个已有模型
	- **参数**： conf: Configuration Angel任务相关配置和算法配置信息
	- **返回值**：无

4. **predict**

	- **定义**：```predict(conf: Configuration)```
	- **功能描述**：启动Angel app，计算预测结果
	- **参数**：conf: Configuration Angel任务相关配置
	- **返回值**：无

5. **predict（default implementation）**

	- **定义：** ```predict(conf: Configuration, model: MLModel, taskClass: Class[_ <: PredictTask[_, _, _]]): Unit```
	- **功能描述：** 启动Angel app 并使用增量训练的方式更新一个旧模型。该方法封装了具体的Angel ps/worker启动以及模型加载/存储过程，子类可直接引用
	- **参数**： conf: Configuration Angel任务相关配置
	- **返回值**：无