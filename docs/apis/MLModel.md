# MLModel
表示一个算法的模型。 一个算法模型一般由一个或者多个矩阵组成，Angel把矩阵对象封装成[PSModel](PSModel.md)。

## 构造方法

- 定义：```def this(psModels: Map[String, PSModel[_]])```

- 参数：psModels: Map[String, PSModel[_] 一个<矩阵名, PSModel对象>映射列表

## predict
- 定义：```def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult]```


- 功能描述：根据模型和预测数据得到预测的结果，需要由具体的模型来实现预测流程


- 参数：storage: DataBlock[LabeledData] 预测数据


- 返回值：DataBlock[PredictResult] 预测结果

## setSavePath
- 定义：```def setSavePath(conf: Configuration)```


- 功能描述：设置模型中的哪些部分需要保存以及保存的路径等


- 参数：conf: Configuration 任务配置信息


- 返回值：无

## setLoadPath
- 定义：```def setLoadPath(conf: Configuration)```


- 功能描述：设置模型中的哪些部分需要加载以及加载的路径等


- 参数：conf: Configuration 任务配置信息


- 返回值：无

## addPSModel
- 定义：```def addPSModel(name: String, psModel: PSModel[_])```


- 功能描述：在模型中新增一个PSModel


- 参数：name: String 矩阵名；psModel: PSModel[_] 矩阵对应的PSModel对象


- 返回值：无

## getPSModel
- 定义：```def getPSModel(name: String): PSModel[_]```


- 功能描述：根据矩阵名获取对应的PSModel对象


- 参数：name: String 矩阵名


- 返回值：PSModel对象

## getPSModels
- 定义：```def getPsModels: Map[String, PSModel[_]]```


- 功能描述：获取该模型包含的所有PSModel对象


- 参数：无


- 返回值：<矩阵名，PSModel对象>映射列表
