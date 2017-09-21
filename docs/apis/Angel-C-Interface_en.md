Currently, many main-stream deep-learning libraries are implemented in C/C++. In order to make Angel parameter server available for these libraries, it is necessary to provide C/C++ interfaces for these libraries to start Angel and call the corresponding interfaces from internal.

Angel is developed with Java, and currently has implemented a basic interface that can start PSAgent, send model delta and get parameters.

* need JNI if called with C
* support multithreading environment; The JNI JavaVM objects can be shared across threads, whereas JNIEnv can't
* abstract out the C version of the PSAgent and MatrixClient classes or interfaces
* PSAgent ensures that each process starts only one instance

The specific interfaces are as follows:

# C/C++ Interfaces

## PSAgent Initialization and Start-up

1. Initialization

	``` C++
		PSAgent psAgent = new PSAgent(conf, masterIp, masterPort, clientIndex);
		conf:Configuration //Angel system configuration
		masterIp:String //Angel app master IP address
		masterPort:int //Angel app master port
		clientIndex:int //client index (globally unique) 
	```
2. Start-up

	```C++
	psAgent.initAndStart();
	```

## Matrix Operations
	
1. getMatrixClient
		
	```C++
		MatrixClient matrixClient = psAgent.getMatrixClient(matrixName, taskIndex);
		matrixName:String
		taskIndex:int task //index the (globally unique) task execution unit, used by sync control (BSP, SSP)
	```

2. Matrix Interfaces

	```C++
		//Set every element in the matrix to a random number
		public void random(int range) throws Exception;

		//Set every element in the matrix to 0
		public void zero() throws Exception;

		//Update the matrix
		public void increment(double[] delta) throws Exception;

		//Update the matrix by index
		public void increment(int[] indexes, double[] delta) throws Exception;

		//Get the matrix by index
		public double[] get(int[] indexes) throws Exception;

		//Get the matrix
		public double[] get() throws Exception;

		//Update the model delta in PSAgent's local cache to PS
		public Future<VoidResult> flush() throws Exception;

		//Update the model delta in PSAgent's local cache to PS and update the clock
		public Future<VoidResult> clock() throws Exception;

		//Aggregate UDF, for example, sum, max
		public AggrResult aggr(AggrFunc func) throws Exception;

		//UDF for matrix updating, for example, scalar, increment
		public Future<VoidResult> update(UpdaterFunc func) throws Exception;
	```
