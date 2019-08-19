# KMeans

> KMeans is a method that aims to cluster data in K groups of equal variance. The conventional KMeans algorithm has performance bottleneck; however，when implemented with PS, KMeans achieves the same level of accuracy with better performance.

## 1. Introduction 

The KMeans algorithm assigns each data point to its *nearest* cluster, where the *distance* is measured between the data point and the cluster's *centers*. In general, Kmeans algorithm is implemented in an iterative way as shown below:  

![kmeans](../img/kmeans.png)   

where, ![xi](../img/xi.png) is the ith sample and ![ci](../img/ci.png) is its nearest cluster; ![miu_j](../img/miu_j.png) is the centers of the jth cluster. 


## Mini-batch KMeans
"Web-Scale K-Means Clustering"[1] proposes an improved KMeans algorithm to address the latency, scalability and sparsity requirements in user-facing web applications, using mini-batch optimization for training. As shown below:

![mini_batch_kmeans](../img/mini_batch_kmeans.png)


## 2. Distributed Implementation on Angel

### Model Storage
KMeans on Angel stores the K centers and K-centers counts on ParameterServer，using a K×N matrix represents the K centers and a K×1 vector represents the K-centers counts, where K is the number of clusters and N is the dimension of data，i.e. number of features. 

### Model Updating
KMeans on Angel is trained in an iterative way; during each iteration, the centers are updated by mini-batch. 

### Algorithm
KMeans on Angel algorithm as follows:
 
![KMeans_on_Angel](../img/KMeans_on_Angel.png)  


## 3. Execution & Performance

### Input Format

* Data format is set in "ml.data.type", which supports "libsvm", "dense" and "dummy" formats. For details, see [Angel Data Format](data_format_en.md)

### Parameters

* IO Parameters
  * ml.feature.index.range: number of features
  * ml.data.type: [Angel Data Format](data_format_en.md), can be "dummy" or "libsvm"
  * angel.train.data.path: input path for train
  * angel.predict.data.path：input path for predict
  * angel.save.model.path: save path for trained model
  *	angel.predict.out.path：output path for predict
  * angel.log.path: save path for the log
  
* Algorithm Parameters
  * ml.epoch.num: number of iterations
  * ml.minibatch.size：: samples for mini-batch
  * ml.kmeans.center.num: K, number of clusters
  * ml.kmeans.c：learning rate
  
* Resource Parameters
  * angel.workergroup.number: number of workers  
  * angel.worker.memory.mb: worker's memory requested in G   
  * angel.worker.task.number: number of tasks on each worker, default is 1   
  * angel.ps.number: number of PS 
  * angel.ps.memory.mb: PS's memory requested in G  

###  **Submit Command**    
see [data](http://ufldl.stanford.edu/housenumbers/)
* **Training Job**
    
	```java
	$ANGEL_HOME/bin/angel-submit \
		--action.type=train \
		--angel.app.submit.class=com.tencent.angel.ml.core.graphsubmit.GraphRunner  \
		--ml.model.class.name=com.tencent.angel.ml.clustering.kmeans.Kmeans \
		--ml.inputlayer.optimizer=KmeansOptimizer \
		--angel.train.data.path=$input_path \
		--angel.save.model.path=$model_path \
		--angel.log.path=$log_path \
		--angel.output.path.deleteonexist=true \
		--ml.data.type=libsvm \
		--ml.model.type=T_DOUBLE_DENSE \
		--ml.kmeans.center.num=$centerNum  \
		--ml.kmeans.c=0.15 \
		--ml.epoch.num=10 \
		--ml.feature.index.range=$featureNum \
		--ml.feature.num=$featureNum \
		--angel.workergroup.number=4 \
		--angel.worker.memory.mb=5000  \
		--angel.worker.task.number=1 \
		--angel.ps.number=4 \
		--angel.ps.memory.mb=5000 \
		--angel.job.name=kmeans_train \
		--angel.worker.env="LD_PRELOAD=./libopenblas.so" \
		--ml.optimizer.json.provider=com.tencent.angel.ml.core.PSOptimizerProvider
	```

* **Prediction Job**

	```java
	$ANGEL_HOME/bin/angel-submit \
		--action.type=predict \
		--angel.app.submit.class=com.tencent.angel.ml.core.graphsubmit.GraphRunner  \
		--ml.model.class.name=com.tencent.angel.ml.clustering.kmeans.Kmeans \
		--ml.inputlayer.optimizer=KmeansOptimizer \
		--angel.predict.data.path=$predictdata \
		--angel.load.model.path=$modelout \
		--angel.predict.out.path=$predictout \
		--angel.output.path.deleteonexist=true \
		--angel.log.path=$logpath \
		--ml.data.type=libsvm \
		--ml.model.type=T_DOUBLE_DENSE \
		--ml.kmeans.center.num=$centerNum \
		--ml.feature.index.range=$featureNum \
		--ml.feature.num=$featureNum \
		--angel.workergroup.number=4 \
		--angel.worker.memory.mb=5000  \
		--angel.worker.task.number=1 \
		--angel.ps.number=4 \
		--angel.ps.memory.mb=5000 \
		--angel.psagent.cache.sync.timeinterval.ms=500 \
		--angel.job.name=kmeans_predict \
		--angel.worker.env="LD_PRELOAD=./libopenblas.so" \
		--ml.optimizer.json.provider=com.tencent.angel.ml.core.PSOptimizerProvider
	```

### Performance
* data：SVHN，3×10^3 features，7×10^4 samples
* resource：
	* Angel：worker：4，5G memory，1 task； ps：4，5G memory
	
## 4. References
[1] Sculley D. Web-scale k-means clustering[C]// International Conference on World Wide Web, WWW 2010, Raleigh, North         	Carolina, Usa, April. DBLP, 2010:1177-1178.