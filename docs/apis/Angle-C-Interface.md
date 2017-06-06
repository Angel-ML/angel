当前主流的深度学习库，一般使用C/C++来实现，为了让它们能使用Angel作为参数服务器，Angel需要提供C/C++接口，让深度学习框架，可以从它们内部，启动Angel并调用相应接口，实现深度学习的参数服务器。


Angel目前基于Java实现，目前已经实现了一个基本的可以启动PSAgent，并可以发送更新以及获取参数。

* 用C来调用，需要借助JNI
* 支持多线程环境，JNI的JavaVM对象可以在线程之间共享，但JNIEnv不可以。
* 抽象出C版本的PSAgent和MatrixClient类或接口
* PSAgent确保每个进程只启动一个实例

具体接口如下：

# C/C++接口

## PSAgent初始化和启动

1. 初始化

	``` C++
		PSAgent psAgent = new PSAgent(conf, masterIp, masterPort, clientIndex);
		conf:Configuration //Angel系统配置
		masterIp:String //Angel app master IP地址
		masterPort:int //Angel app master 端口
		clientIndex:int //client 编号（全局唯一）
	```
2. 启动

	```C++
	psAgent.initAndStart();
	```

## Matrix操作
	
1. 获取矩阵客户端
		
	```C++
		MatrixClient matrixClient = psAgent.getMatrixClient(matrixName, taskIndex);
		matrixName:String //矩阵的名字
		taskIndex:int task //编号（全局唯一）任务执行单元编号，用作同步控制（BSP，SSP）之用
	```

2. Matrix接口说明

	```C++
		//将矩阵每一个元素置为随机数
		public void random(int range) throws Exception;

		//将矩阵每一个元素置为0
		public void zero() throws Exception;

		//更新矩阵
		public void increment(double[] delta) throws Exception;

		//按索引更新矩阵
		public void increment(int[] indexes, double[] delta) throws Exception;

		//按索引获取矩阵
		public double[] get(int[] indexes) throws Exception;

		//获取矩阵
		public double[] get() throws Exception;

		//将PSAgent本地缓存的矩阵更新刷新到PS
		public Future<VoidResult> flush() throws Exception;

		//将PSAgent本地缓存的矩阵更新刷新到PS，同时更新clock值
		public Future<VoidResult> clock() throws Exception;  

		//聚合UDF，例如sum，max等
		public AggrResult aggr(AggrFunc func) throws Exception;

		//矩阵更新UDF，例如scalar，increment等
		public Future<VoidResult> update(UpdaterFunc func) throws Exception;
	```


