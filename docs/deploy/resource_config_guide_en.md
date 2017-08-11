# Angel Resource Configuration Guide

---

> Angel is a distributed machine-learning system based on the parameter-server (PS) paradigm. The introduction of PS simplifies the computational complexity and improves the operational speed; in the meantime, however, it increases the complexity of resource configuration. We create this file to help guide users configure the PS system properly to achieve high performance of their algorithms. 

We list resource parameters for Angel jobs. Sensible configurations of these parameters, based on the application-specific information such as data size, model design, and machine-learning algorithms, can significantly improve the operational efficiency.

* **Master**
	* Memory size of master
	* Number of Master's CPU vcores
* **Worker**
	* Number of workers
	* Memory of single worker
	* Number of single worker's CPU vcores
* **Parameter Server**
	* Number of PS
	* Memory of single PS
	* Number of single PS's CPU vcores


## Master

Because Angel master is lightweight, in most cases, the default configuration is sufficient. Since the master does not host the computations, it does not need more resources than the default allocation unless there will be too many workers. In contrast, configurations of the worker and PS resources need more care. 


## Worker 

### 1. **Estimating the Number of Workers**

Angel can configure number of workers autonomously to achieve the best result for data parallelization. In general, the number of workers is determined by the total amount of data in the computation. We suggest the typical range of data that a single worker processes to be **between 1GB and 5GB** . Please note that the estimation is for uncompressed data only, and needs to be multiplied by the data compression ratio for other formats --- we use the same principle for other estimations when applicable.

### 2. **Estimating the Memory of a Single Worker**

The comparative usage of worker memory is shown below:

![][1]
 
* **Model Partitions** 
	- **model**: part of the model being pulled from the PS and to be computed on the current worker
	- **model delta**: model update computed by current worker (per task)
	- **merged model delta**: model updates merged from all tasks on the current worker
* **System Buffer**
	- **system buffer**: ByteBuf pool used by Netty framework, for example

* **Data**
	- **training data**
		- the **ratio** of memory usage of the original data size to formatted data size is **1.5:1**; for instance, original data that is 1.5GB in size will actually occupy 1GB memory when formatted and loaded into the memory
		- the above ratio is true for both dummy and libsvm
		- in case of memory shortage, local disks can be used for storing formatted data

* **Equations for Estimation**  

	Let's define the following variables:

	* `N`: number of tasks running on a single worker
	* `Sm`: size of model used in training
	* `Smd`: size of model update per iteration, per task
	* `Smmd`: size of merged model updates
	* `St`: memory used for training data
	* `Sb`: memory used by the system

	Then, the worker memory can be estimated by ```Me = N * Sm + 2 * N * Smd + Smmd + St + Sb```

	The system memory can be roughly estimated by ```Sb = Sm + Smmd + 1```

	Summing the two up, the overall estimation is ```Me = (N + 1) * Sm + 2 * N * Smd + 2 * Smmd + St + 1```

### 3. **Estimating the Number of Worker CPU VCores**
In contrast to the memory parameters, number of CPU vcores only affects the job's operational efficiency, but not its accuracy.  We suggest adjusting the number of CPU vcores and memory based on the resources on the actual machine, for example: 

> Assuming the machine has 100G total memory and 50 CPU vcores; if the worker's memory is configured to be 10G/20G, then the number of CPU vcores can be configured to 5/10

## **PS Resources**

### 1. Estimating the Number of PS

Number of PS depends on the **number of workers**, the **model types** (see `Model Partitions` section below for definition) and **model complexity**. 

> In general, PS number should increase if the number of workers or model complexity increase. We suggest setting the number of PS to be between 1/5 and 4/5 of the number of workers. In fact, the number of PS should be configured in an algorithm-specific way.  

### 2. Estimating the Memory of PS

The comparative usage of PS memory is shown below: 
	![][2]
 
- **Model Partitions**: partitions of the model loaded on the PS

	* there are two model types: sparse and dense; sparse model is represented as a Map object, whereas dense model is represented as a double array, and they require different sizes of memory.
	
- **System Buffer**: ByteBuf pool used by Netty framework, among others

	* Large sizes for send and receive buffers are needed due to PS's interactions with multiple workers with large amount of data; therefore, as a general case, memory consumed by the system itself is much greater in size than memory consumed by the model partitions


- **Equations for Estimation**

	* `Smp`: size of model partitions 
	* `Sw`: size of model being pulled per iteration, per task
	* `N`: number of workers

	PS memory is roughly estimated by: ```Smp + 2 * N * Sw```

### 3. Estimating the Number of PS CPU VCores

This is similar to the estimation for workers, but needs additional analyses of the algorithm. In gneral, if there is intense computation on the PS side, number of vcores needs to be raised correspondingly. 

# Logistic Regression Parameter Configuration

We take LR as an example to demonstrate the parameter configurations. By default, LR is dense and uses double arrays to store the model and model partitions.  

Assuming 300GB training data and 100M features: 

* **Worker Side**

If we configure 100 workers, each of which running one task, then the estimated memory for each single worker is:

```
Sm = Smd = Smmd = 8 * 10 ^ 8 / 10 ^ 9 = 0.8GB
St = 300 / 100 / 1.5 = 2GB
N = 1
Me = (N + 1) * Sm + 2 * N * Smd + 2 * Smmd + St + 1 = 2 * 0.8 + 2 * 0.8 + 2 * 0.8 + 2 + 1 = 7.8GB
```

* **PS Side**

We round up the worker memory to 8GB, set the number of PS to 20 (1/5 of the number of workers). Now, each PS loads 5M model partitions, thus the estimated memory requirement for each PS is: 

```
Smp = Sw = 8 * 5 * 10 ^ 6 / 10 ^ 9 = 0.04GB
Me = 0.04 + 2 * 100 * 0.04 = 8.04GB
```

We round the PS memory to 8GB. Assuming each machine has 128G memory and 48 CPU vcores, each worker needs the following CPU vcores:

```
Vw = 48 / 128 * 8 = 3
```

Number of CPU vcores for the PS is:
```
Vp = 48 / 128 * 8 = 3
```

 [1]: ../img/worker_memory.png
 [2]: ../img/ps_memory.png

