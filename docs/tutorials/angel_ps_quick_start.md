# Angel快速入门指南
## 准备知识
这篇文档帮助你快速开始编写运行在Angel-PS架构上的程序，开始之前，你最好掌握以下能力：
  
* 会编写简单的Scala或者Java代码  
* 至少掌握向量、矩阵的基础知识，了解其定义和基础计算。    
* 最好对机器学习算法有一定了解。如果没有学习过机器学习算法，也没有关系，可以从这篇文档开始。   


大多数的机器学习算法都可以抽象成向量、矩阵间的运算，用向量、矩阵表示学习数据和算法模型。Angel-PS实现了基于参数服务器的矩阵计算，将分布在多台PS Server上的参数矩阵抽象为PSModel，你只需要完成PSModel的定义、实现其计算过程，就可以实现一个运行在参数服务器上的简单算法。在开始编程前，先来了解一些基础知识：  

### Angel-PS架构
简单的Angel-PS架构如下图所示，PS是存储矩阵参数的多台机器，向计算节点提供矩阵参数的拉取、更新服务。每个worker是一台机器，一个worker运行一或多个task，每个task是一个计算节点。机器学习的算法模型一般以迭代的方式训练，每次迭代worker从PS拉取最新的参数，计算一个更新值，推送给PS。  
![Architecture for LDA on Angel](../img/brief_structure.png)

### Angel接口说明
写一个简单的运行在Angel-PS上的算法，你需要了解以下几个类：

 * PSModel类  
  PSModel是存储在PS上的矩阵的抽象，封装了矩阵的元信息，提供worker对PS上参数矩阵操作的接口。在提交Angel任务时，首先要定义矩阵的数据类型、维度等元信息。在task迭代计算的过程中，、PSModel的getRow方法将矩阵参数拉取到worker、increment方法将worker计算得到的更新值推送给PS。
 * MLModel类  
  MLModel是算法模型的抽象，每个算法模型有一或多个PSModel,通过addPSModel方法添加。
 * MLRunner类   
  MLRunner是Angle-PS的启动类，算法需要实现继承MLRunner的runner类启动。
 * Task类   
  Task是算法的计算单元，每个Task是一个线程，算法的训练过程在Task中实现。算法的训练过程、和模型预测需要分别继承TrainTask、PredictTask两个类。

## 开始你的第一个Angel算法: myLR
本节以实现简单的Logistic Regression算法为例，指导你完成第一个Angel-PS算法，本示例的代码可以在example.quickStart里找到。   
逻辑回归算法的模型可以抽象为一个维度为1×N的矩阵，即一个N维向量，记为w。用梯度下降法训练LR模型，每次迭代，task从PS拉取最新的模型w,然后计算得到梯度△w，再将△w推送给PS。首先，要定义LR算法的模型；然后，完成LR模型迭代计算的过程；最后，实现启动类把这个任务提交到集群。

### 定义LR模型
实现myLRModel类继承MLModel，通过addPSModel添加一个1×N维的PSModel给LRModel，在setSavePath方法中，设置运算结束后LR模型的保存路径。N的值、保存路径都可以通过conf配置。
``` java 
class myLRModel(ctx: TaskContext, conf: Configuration) extends MLModel(ctx){
  val N = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)

  val weight = new PSModel[DenseDoubleVector]("mylr.weight", 1, N)
  weight.setAverage(true)
  addPSModel(weight)

  override 
  def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)
    if (path != null)
      weight.setSavePath(path)
  }

  override 
  def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult] = ???

  override 
  def setLoadPath(conf: Configuration): Unit = ???
}
```

### 迭代训练LR
模型的训练在task中完成，我们定义myLRTrainTask来完成LR的模型的训练过程。myLRTrainTask需要继承TrainTask类并实现数据预处理方法：parse、训练方法：train。
* 解析输入数据    
   在模型开始训练前，输入的每一行文本被解析为一条训练数据，解析方法在parse方法里实现，此处我们使用DataParser解析dummy格式的数据。
```
  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    DataParser.parseVector(key, value, feaNum, "dummy", negY = true)
  }
```
可以通过task的dataBlock访问预处理后的数据。
* 训练
  Angel会自动执行TrainTask子类的train方法，我们在myLRTrainTask的train方法中完成模型训练过程。在这个简易的LR算法例子中，我们先实例化myLRModel模型对象model，然后开始迭代计算。每次迭代，task从PS拉取模型的参数weight，然后用训练数据计算得到梯度grad，把grad推送给PS。PS上weight的更新会自动完成。推送grad后，需要clock()、incIteration()。
```java
override
def train(ctx: TaskContext): Unit = {
	
	// A simple logistic regression model
	val model = new myLRModel(ctx, conf)
	
	// Apply batch gradient descent LR iteratively
	while (ctx.getIteration < epochNum) {
	  // Pull model from PS Server
	  val weight = model.weight.getRow(0)
	
	  // Calculate gradient vector
	  val grad = bathGradientDescent(weight)
	
	  // Push gradient vector to PS Server
	  model.weight.increment(grad.timesBy(-1.0 * lr))
	
	  // LR model matrix clock
	  model.weight.clock.get
	
	  // Increase iteration number
	  ctx.incIteration()
	}
}
```
  
### 启动训练任务
前面，我们定义了LR模型，实现了它的训练过程。现在，还需要实现Runner类将训练这个模型的任务提交到集群。  
定义myLRRunner类继承MLRunner，在train方法中提交我们的myLRModel的模型类、和myLRTrainTak训练类就可以了。
```java
class myLRRunner extends MLRunner{

  /** Training job to obtain a model*/
  override
  def train(conf: Configuration): Unit = {
    train(conf, new myLRModel(null, conf), classOf[myLRTrainTask])
  }

  override 
  def incTrain(conf: Configuration): Unit = ???

  override 
  def predict(conf: Configuration): Unit = ???
}
```

### 运行任务
* 本地集群
首先，要创建配置文件对象conf，配置myLR任务运行需要的资源、算法、IO参数。然后通过myLRRunner将这个任务提交到本地集群。
    
```java
public static void main(String[] args) {
  Configuration conf = setConf();

  myLRRunner runner = new myLRRunner();

  runner.train(conf);
}
```
* Yarn集群
可以通过以下命令向Yarn集群提交刚刚完成的算法任务
```
./bin/angel-submit \
-- action.type train \
-- angel.app.submit.class com.tencent.angel.example.quickStart.myLRRunner  \
-- angel.train.data.path $input_path \
-- angel.save.model.path $model_path \
-- ml.epoch.num 10 \
-- ml.feature.num 10000 \
-- ml.data.type dummy \
-- ml.learn.rate 0.001 \
-- angel.workergroup.number 3 \
-- angel.worker.memory.mb 8000  \
-- angel.worker.task.number 3 \
-- angel.ps.number 1 \
-- angel.ps.memory.mb 5000 \
-- angel.job.name myLR
```




