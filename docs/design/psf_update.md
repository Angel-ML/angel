# UpdateFunc(更新型函数)


## 原理

1. PS Client（参数服务器客户端）进行请求划分，生成一个请求列表，这个请求列表中的每一个请求都和一个模型参数分区对应。

2. 将请求列表中的所有请求发送给模型参数分区所在的PS实例。PS实例以模型参数分区为单位执行参数获取和更新操作，并返回相应的结果。

3. 等待所有请求完成后返回


## **定义**

* **接口**

```Java
Future<VoidResult> update(UpdaterFunc update) throws AngelException;
```

* **参数**
	* 参数类型是一个UpdaterFunc对象，该对象封装了update psf方法的参数和执行流程：

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

	* UpdateFunc对象的参数类型为UpdateParam
	   * UpdateParam对象与GetParam对象类似，它除了包含update的具体参数外，也有一个split方法，该方法的作用是将全局的update参数按照矩阵分区进行划分，得到的结果是一个分区update参数列表，即PartitionUpdateParam对象列表。

## 执行流程

与get psf不同，update psf的执行流程只有一步：

* 即以矩阵分区为单位分别进行update操作，这个过程partitionUpdate方法表示。

update psf没有具体的返回值，只返回给应用程序一个Future，应用程序可以选择是否等待操作完成。

update型psFunc执行流程需要PS Client和PS共同完成。上述提到的

* UpdaterParam划分是在Worker执行
* partitionUpdate方法是在PSServer端执行

具体的流程如下图所示，左子图表示Worker处理流程，右子图表示PS Server处理流程：

![](../img/psf_update.png)

## 编程样例

* [com.tencent.angel.ml.matrix.psf.update.RandomUniform](https://github.com/Tencent/angel/blob/master/angel-ps/psf/src/main/java/com/tencent/angel/ml/matrix/psf/update/RandomUniform.java): 将矩阵某一行设置为指定范围随机数的例子


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

将代码编译后打成jar包，在提交任务时通过参数angel.lib.jars上传该jar包，然后就可以在应用程序中调用了。调用方式如下：

```Java
	Random randomFunc = new RandomUniform(new RandomParam(matrixId, rowIndex, 0.0, 1.0));
	psModel.update(randomFunc).get();
```

## Update函数库

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

