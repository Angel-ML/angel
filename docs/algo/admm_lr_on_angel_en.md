# SparseLogisticRegression

SparseLogisticRegression uses L1 regularization that yields sparse solutions, compared to LogisticRegression that uses L2 regularization yielding "dense" solutions. 

## ADMM

In Angel, we use ADMM algorithm [1] to solve optimization of the objective in LogisticRegression. Comparing to SGD, ADMM converges to modest accuracy within a few tens of iterations [1]; however, ADMM can be very slow to converge to high accuracy. A common practice is to combine ADMM with other algorithms to improve the rate of convergence. In Angel, we use LBFGS in Breeze to optimize for the sub-models in each iteration. 


## Execution

### Input Format

Data fromat is set in "ml.data.type"; feature vector's dimension is set in "ml.feature.num".

SparseLR on Angel supports "libsvm" and "dummy" types. For details, see [Angel Data Format](data_format_en.md)


###  Parameters
* Algorithm Parameters 
  * ml.epoch.num: number of iterations/epochs   
  * ml.reg.l1: coefficient of the L1 penalty
  * rho: rho
  * ml.worker.thread.num: number of threads on each worker (for parallel training)

* I/O Parameters
  * angel.train.data.path: input path
  * ml.feature.num: number of features  
  * ml.data.type: [Angel Data Format](data_format_en.md), supporting "dummy" and "libsvm"    
  * angel.save.model.path: save path for trained model 
  * angel.log.path: save path for the log
   
* Resource Parameters
  * angel.workergroup.number: number of workers   
  * angel.worker.memory.mb: worker's memory requested in G   
  * angel.worker.task.number: number of tasks on each worker, default is 1    
  * angel.ps.number: number of PS
  * angel.ps.memory.mb: PS's memory requested in G

## Performance
We use Tencent's internal data sets to compare the performance of Angel and Spark as a platform for SparseLR. 

* Training Data

| Data Set | Data Set Size | Sample Size | Number of Features | Task |
|:------:|:----------:|:--------:|:--------:|:-------:|
| XXX  |    350GB    |   100M  |   50M   | Binary |


* **Environment**

	The experiment was run on Tencent's Gaia cluster (Yarn), and each instance has the following configuration:

	* CPU: 2680 * 2
	* Memory: 256 GB
	* Network: 10G * 2
	* Disk: 4T * 12 (SATA)


* **Variable Setting**

    * Spark: 200 executors, 20G memory; driver 20G memory
    * Angel：100 workers, 10G memory; 50 ps, 5G memory
    
* **Experiment Result**

    ![](../img/admm_lr.png)


## Reference
1. Boyd S, Parikh N, Chu E, et al. [Distributed optimization and statistical learning via the alternating direction method of multipliers](https://pdfs.semanticscholar.org/905b/cb57493c8b97b216bc6786aa122e1ad608b0.pdf). Foundations and Trends® in Machine Learning, 2011, 3(1): 1-122.
