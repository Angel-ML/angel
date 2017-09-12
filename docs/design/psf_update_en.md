# UpdateFunc


## Principles

1. PS Client divides the requests to generate a request list, where each request corresponds to a model parameter partition.

2. PS Client sends every request in the list to the PS instance that has the corresponding partition. The PS instance gets and updates the parameters in units of model parameter partitions, and returns the result.

3. Wait for all requests to be returned


## Definitions

* **Interface**

```Java
Future<VoidResult> update(UpdaterFunc update) throws AngelException;
```

* **Parameters**
	* The parameter type is an UpdaterFunc object, which encapsulates the parameters and process of the `update psf` method:

		```Java
		public abstract class UpdaterFunc {
			private final UpdaterParam param;
			public UpdaterFunc(UpdaterParam param) {
		  		this.param = param;
			  }
			public UpdaterParam getParam() {return param;}
			public abstract void partitionUpdate(PartitionUpdaterParam partParam);
		}
		```

	* The parameter type of an UpdateFunc object is UpdateParam
	   * Similar to the GetParam object, UpdateParam defines a `split` method, which divides the overall `update` parameters to a list of `update` parameters for their corresponding model partitions (list of PartitionUpdateParam objects). 


## Execution Process

Different from `get psf`, `update psf`'s process consists of only one step:

* `update` in units of model partitions (partitionUpdate)

`update psf` has no specific return value, but returns a Future to the application; the application then decides whether to wait for the operations to complete.

The execution process of update type psFunc requires the PS Client and PS to work together:

* UpdaterParam division execute on the worker
* partitionUpdate method execute on the PSServer

The specific process is shown below, with the chart on the left showing the process on the worker, the chart on the right showing the process on the server:

![](../img/psf_update_en.png)

## Sample Code

* [com.tencent.angel.ml.matrix.psf.update.RandomUniform](https://github.com/Tencent/angel/blob/master/angel-ps/psf/src/main/java/com/tencent/angel/ml/matrix/psf/update/RandomUniform.java): an example that assigns a specified range of random numbers to a row:


	```Java
		public class RandomUniform extends MMUpdateFunc {
			...
		  @Override
		  protected void doUpdate(ServerDenseDoubleRow[] rows, double[] scalars) {
		    Random rand = new Random(System.currentTimeMillis());
		    try {
		      rows[0].getLock().writeLock().lock();
		      double min = scalars[0];
		      double max = scalars[1];
		      double factor = max - min;

		      DoubleBuffer data = rows[0].getData();
		      int size = rows[0].size();
		      for (int i = 0; i < size; i++) {
		        data.put(i, factor * rand.nextDouble() + min);
		      }
		    } finally {
		      rows[0].getLock().writeLock().unlock();
		    }
		  }
		}
	```

Compile the code and create the jar to be uploaded through `angel.lib.jars` when submitting the application; the new UpdateFunc can then be called in the program, for instance: 

```Java
	Random randomFunc = new RandomUniform(new RandomParam(matrixId, rowIndex, 0.0, 1.0));
	psModel.update(randomFunc).get();
```

## Update Library

	* **Abs**

		* 功能：将矩阵某一行每一个元素的绝对值赋值给另外一行的相应元素
		* 参数：矩阵id，from 行号， to行号
		* 返回值：无

	* **Add**
		* 功能：将矩阵某两行之和赋值给另外一行
		* 参数：矩阵id，from 行号1，from行号2，to行号
		* 返回值：无

	* **AddS**
		* 功能：将矩阵某一行元素都加上一个标量后赋值给另外一行的相应元素
		* 参数：矩阵id，from 行号1，to行号，标量值
		* 返回值：无

	* **Axpy**
		* 功能：y=Ax+y, x,y为向量，A为标量
		* 参数：矩阵id，代表x向量的行的行号，代表y向量的行的行号，标量值A
		* 返回值：无

	* **Ceil**
		* 功能：将矩阵中的某一行的个元素向上取整得到的值赋值给另外一行的相应元素
		* 参数：矩阵id，from行号，to行号
		*返回值：无

	* **Copy**
		* 功能：将矩阵中的某一行的个元素的值赋值给另外一行的相应元素
		* 参数：矩阵id，from行号，to行号
		* 返回值：无
		
	* **Div**

		* 功能：将矩阵中的某两行的对应元素相除得到的值赋值给另外一行的相应元素
		* 参数：矩阵id，from行号1，from行号2，to行号
		* 返回值：无

	* **DivS**
	    * 功能：将矩阵中的某一行的每一个元素除以一个标量得到的值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号，标量值
	    * 返回值：无

	* **Exp**
	    * 功能：将矩阵中的某一行的每一个元素计算指数（以e为底，元素值为幂）运算，将值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号
	    * 返回值：无

	* **Expm1**
	    * 功能：将矩阵中的某一行的每一个元素以e为底计算指数然后减去1，将得到的值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号
	    * 返回值：无

	* **Fill**
	    * 功能：将矩阵中的某一行的每一个元素都置为一个标量值
	    * 参数：矩阵id，行号，标量值
	    * 返回值：无

	* **Floor**
	    * 功能：将矩阵中的某一行的每一个元素向下取整，将得到的值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号
	    * 返回值：无

	* **Increment**
	    * 功能：将矩阵中的某一行的每个元素加上一个标量值
	    * 参数：矩阵id，from行号，标量值数组，该数组长度必须与向量大小相等
	    * 返回值：无

	* **Log**
	    * 功能：将矩阵中的某一行的每个元素求取自然对数， 将得到的值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号
	    * 返回值：无

	* **Log10**
	    * 功能：将矩阵中的某一行的每个元素求取以10为底对数，将得到的值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号
	    * 返回值：无

	* **Log1p**
	    * 功能：将矩阵中的某一行的每个元素加上1后求取为底对数，将得到的值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号
	    * 返回值：无

	* **Map**
	    * 功能：将矩阵中的某一行的每个元素进行某种计算，将得到的值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号，计算函数（该计算函数拥有一个    * 参数，即    * 参数为元素的值）
	    * 返回值：无

	* **MapWithIndex**
	    * 功能：将矩阵中的某一行的每个元素进行某种计算，将得到的值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号，计算函数（该计算函数拥有二个    * 参数，即元素的下标和值）
	    * 返回值：无

	* **MaxA**
	    * 功能：将矩阵中的某一行的每个元素与一个标量值进行比较，然后将较大的值赋值给该行的相应元素
	    * 参数：矩阵id，行号，标量值数组（该数组必须与行大小相等）
	    * 返回值：无

	* **MaxV**
	    * 功能：将矩阵中的某两行的对应元素进行比较，然后将较大的值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号1，from行号2，to行号
	    * 返回值：无

	* **MinA**
	    * 功能：将矩阵中的某一行的每个元素与一个标量值进行比较，然后将较小的值赋值给该行的相应元素
	    * 参数：矩阵id，行号，标量值数组（该数组必须与行大小相等）
	    * 返回值：无

	* **MinV**
	    * 功能：将矩阵中的某两行的对应元素进行比较，然后将较小的值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号1，from行号2，to行号
	    * 返回值：无

	* **Mul**
	    * 功能：将矩阵中的某两行的对应元素相乘，然后将值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号1，from行号2，to行号
	    * 返回值：无

	* **MulS**

	    * 功能：将矩阵中的某一行的每个元素乘以一个标量，然后将值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号，标量值
	    * 返回值：无

	* **Pow**
	    * 功能：将矩阵中的某一行的每个元素做指数运算（指数的底为元素值，幂为一个指定的标量），然后将值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号，标量值
	    * 返回值：无

	* **Put**
	    * 功能：给矩阵的某一行的每一个元素赋值
	    * 参数：矩阵id，行号，标量值数组（该数组大小必须与行大小相等）
	    * 返回值：无

	* **RandomNormal**
	    * 功能：给矩阵的某一行的每一个元素赋一个随机值，值符合高斯分布
	    * 参数：矩阵id，行号，高斯分布平均数，高斯分布标准差
	    * 返回值：无

	* **RandomUniform**
	    * 功能：给矩阵的某一行的每一个元素赋一个随机值，值符合均匀分布
	    * 参数：矩阵id，行号，均匀分布范围下限，均匀分布范围上限
	    * 返回值：无

	* **Round**
	    * 功能：将矩阵中的某一行的每个元素做round计算（求取最接近的整数），然后将值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号
	    * 返回值：无

	* **Scale**
	    * 功能：将矩阵中的某一行的每个元素乘以一个标量
	    * 参数：矩阵id，行号，标量值
	    * 返回值：无

	* **Signum**
	    * 功能：将矩阵中的某一行的每个元素做signum运算（值大于0，返回1.0；小于0，返回-1.0；等于0，返回0），然后将值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号
	    * 返回值：无

	* **Sqrt**
	    * 功能：将矩阵中的某一行的每个元素做平方根运算，然后将值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号，to行号
	    * 返回值：无

	* **Sub**
	    * 功能：将矩阵中的某两行的对应元素做减法，然后将值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号1，from行号2，to行号
	    * 返回值：无

	* **Zip2Map**
	    * 功能：将矩阵中的某两行的对应元素指定运算（由一个函数表示），然后将值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号1，from行号2，to行号，运算函数（该函数有两个    * 参数，分别是两行对应位置上的元素值）
	    * 返回值：无

	* **Zip2MapWithIndex**
	    * 功能：将矩阵中的某两行的对应元素指定运算（由一个函数表示），然后将值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号1，from行号2，to行号，运算函数（该函数有三个    * 参数，分别是元素下标索引，两行对应位置上的元素值）
	    * 返回值：无

	* **Zip3Map**
	    * 功能：将矩阵中的某三行的对应元素指定运算（由一个函数表示），然后将值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号1，from行号2，to行号，运算函数（该函数有三个    * 参数，分别是三行对应位置上的元素值）
	    * 返回值：无

	* **Zip3MapWithIndex**
	    * 功能：将矩阵中的某三行的对应元素指定运算（由一个函数表示），然后将值赋值给另外一行的相应元素
	    * 参数：矩阵id，from行号1，from行号2，to行号，运算函数（该函数有四个    * 参数，分别是元素下标索引，三行对应位置上的元素值）
	    * 返回值：无
