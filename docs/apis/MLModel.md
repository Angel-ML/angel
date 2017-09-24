# MLModel

---

> MLModel表示一个机器学习算法中，需要用到的完整模型集合。一个复杂的机器学习算法，往往需要多个远程模型[PSModel](PSModel.md)协作，为了方便对这些模型进行统一操作和管理，MLModel作为一个抽象类，成为了Angel所有模型的基类

## 功能

MLModel是一个抽象类，它需要在具体的机器学习算法实现中进行继承。它是作为一个容器类，管理具体算法中的所有PSModel，作为一个整体模型被加载，训练和保存。同时，它需要负责Predict的执行逻辑实现。


## 核心方法


*  **predict**
	- 定义：```def predict(storage: DataBlock[predictType]): DataBlock[PredictResult]```
	- 功能描述：根据模型和预测数据得到预测的结果，需要由具体的模型（PSModel）来实现预测流程
	- 参数：storage: DataBlock[predictType] 预测数据
	- 返回值：DataBlock[PredictResult] 预测结果

*  **setSavePath**

	- 定义：```def setSavePath(conf: Configuration)```
	- 功能描述：设置需要保存的模型，以及保存的路径等
	- 参数：conf: Configuration 任务配置信息
	- 返回值：无

  * **setLoadPath**
	- 定义：```def setLoadPath(conf: Configuration)```
	- 功能描述：设置模型的整体加载路径等
	- 参数：conf: Configuration 任务配置信息
	- 返回值：无

*  **addPSModel**

	- 定义：```def addPSModel(name: String, psModel: PSModel[_])```
	- 功能描述：在模型中新增一个PSModel
	- 参数：
		- name: String 模型名
		- psModel: PSModel[_] 对应的PSModel对象
	- 返回值：无

* **getPSModel**

	- 定义：```def getPSModel(name: String): PSModel[_]```
	- 功能描述：根据矩阵名获取对应的PSModel对象
	- 参数：name: String 矩阵名
	- 返回值：PSModel对象

