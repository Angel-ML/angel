# get型PSFunc

## **定义**

* **接口**
	
	```Java
	GetResult get(GetFunc get) throws AngelException;
	```

* **参数**
	* get型psFunc的参数类型是一个GetFunc对象，该对象封装了get psf方法的参数和执行流程：

		```Java
		public abstract class GetFunc {
		  protected final GetParam param;
		  public GetFunc(GetParam param) {
		    this.param = param;
		  }
		  public GetParam getParam() {return param;}
		  public abstract PartitionGetResult partitionGet(PartitionGetParam partParam);
		  public abstract GetResult merge(List<PartitionGetResult> partResults);
		}
		``` 

	* GetFunc对象的参数类型为GetParam
		* GetParam实现了ParamSplit接口，ParamSplit接口定义了一个split方法，该方法的含义是将一个针对整个矩阵的全局的参数划分成一个矩阵分区参数列表。
		* GetParam类型提供了一个默认的split接口实现，也就是针对该矩阵的每一个分区都生成一个矩阵分区的参数。 
		* get psf的矩阵分区参数是一个PartitionGetParam类型。


## 执行流程

get型psFunc的执行流程分为两步，分别由接口partitionGet和merge方法表示。
	
*  partitionGet方法定义了从一个矩阵分区获取所需结果的具体流程，它的返回结果类型为PartitionGetResult
* merge方法定义了如何将各个矩阵分区的结果合并得到完整结果的过程。完整的结果类型为GetResult

这2步分别需要Worker端和PSServer端完成：
	
* **参数划分和Merge方法**是在Worker端执行
* **partitionGet**是在PS端执行
 
具体的流程如下图所示，左子图表示Worker端处理流程，右子图表示PS端处理流程：

![][1]


## 编程样例

由于getFunc的接口太底层，建议普通用户，从AggrFunc开始继承编写psFunc，这样需要Cover的细节较少

* [com.tencent.angel.ml.matrix.psf.aggr.Sum](https://github.com/Tencent/angel/blob/master/angel-ps/psf/src/main/java/com/tencent/angel/ml/matrix/psf/aggr/Sum.java): 获取矩阵某一行的所有值的和



```Java
public final class Sum extends UnaryAggrFunc {
  ...
  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    double sum = 0.0;
    for (PartitionGetResult partResult : partResults) {
      sum += ((ScalarPartitionAggrResult) partResult).result;
    }

    return new ScalarAggrResult(sum);
  }
```

将代码编译后打成jar包，在提交任务时通过参数--angel.lib.jars上传该jar包，然后就可以在应用程序中调用了。调用方式如下：

```Java
	Sum sumFunc = new Sum(new SumParam(matrixId, rowIndex));
	double result = ((SumResult)psModel.get(sumFunc)).getResult();
```


## 内置库

* **Amax**
	* 功能:获取矩阵某一行的元素的最大绝对值
	* 参数:矩阵id，行号
	* 返回值:最大绝对值

* **Amin**

	* 功能:获取矩阵某一行的元素的最小绝对值
	* 参数:矩阵id，行号
	* 返回值:最小绝对值

* **Asum**
	* 功能:获取矩阵某一行的元素的绝对值之和
	* 参数:矩阵id，行号
	* 返回值:指定行的元素绝对值之和

* **Max**
	* 功能:获取矩阵某一行的元素的最大值
	* 参数:矩阵id，行号
	* 返回值:最大值

* **Min**
	* 功能:获取矩阵某一行的元素的最小值
	* 参数:矩阵id，行号
	* 返回值:最小值

* **Sum**
	* 功能:获取矩阵某一行的元素之和
	* 参数:矩阵id，行号
	* 返回值:指定行的元素之和

* **Dot**
	* 功能:获取矩阵某两个行向量的内积
	* 参数:矩阵id，行号1， 行号2
	* 返回值:指定某两个行向量的内积

* **Nnz**
	* 功能:获取矩阵某一行的元素中非零值的总数
	* 参数:矩阵id，行号
	* 返回值:非零值的总数

* **Nrm2**
	* 功能:获取矩阵某一行的元素算术平方根
	* 参数:矩阵id，行号
	* 返回值:算术平方根


  [1]: ../img/psf_get.png
