# AngelClient

---


> AngelClient封装了所有对Angel总操作的主要接口，包括对PSServer和Worker的直接控制。MLRunner通过调用它，完成一次完整的机器学习任务的调用流程。

## 功能

* 对于一个训练的过程，一般来说AngelClient需要启动PSServer，加载模型，并启动Worker运行指定任务，等待到完成后，再保存模型到指定位置的一系列固定动作


## 核心接口

1. **startPSServer**
	- 定义：```void startPSServer() ```
	- 功能描述：启动Angel 参数服务器；该方法会首先启动Angel Master，然后向Angel Master发送启动参数服务器的命令，直到所有的
参数服务器启动成功该方法才会返回
	- 参数：无
	- 返回值：无

2. **loadModel**

	- 定义：```void loadModel(MLModel model) ```
	- 功能描述：加载模型。该方法在不同的功能模式下有不同的流程，目前Angel支持**训练** 和**预测** 两种功能模式，其中训练又可分为**新模型训练**和**模型增量更新**两种方式。在**模型增量更新** 和**预测** 模式下，该方法会首先将已经训练好的模型元数据和参数加载到参数服务器的内存中，为下一步计算做准备；在**新模型训练** 模式下，该方法直接在参数服务器中定义新的模型并初始化模型参数
	- 参数：
		- MLModel model ：模型参数，包括模型划分信息，模型初始化方式等
	- 返回值：无

3.  **runTask**
	- 定义：```void runTask(Class<? extends BaseTask> taskClass) ```
	- 功能描述：启动计算过程。该方法启动Worker和Task，开始执行具体的计算任务
	- 参数
		- Class<? extends BaseTask> taskClass：Task计算流程，一般情况下Angel mllib中的每一种算法都提供了对应的Task实现，但当用户需要修改具体执行流程或者实现新的算法时，需要提供一个自定义的实现
	- 返回值：无
	
4. **waitForCompletion**

	- 定义：```void waitForCompletion() ```
	- 功能描述：等待计算完成。该方法在训练和预测功能模式下有不同的流程。 
		- 在训练模式下，具体的训练计算过程一旦完成，该方法就会返回
		- 在预测模式下，在预测计算过程完成后，该方法会将保存在临时输出目录下的预测结果rename到最终输出目录
	- 参数：无
	- 返回值：无

5. **saveModel**
	- 定义：```void saveModel(MLModel model) ```
	- 功能描述：保存计算好的模型。该方法只有在训练模式下才会有效
	- 参数：无
	- 返回值：无

6. **stop**
	- 定义：```void stop() ```
	- 功能描述：结束任务，释放计算资源，清理临时目录等
	- 参数：无
	- 返回值：无