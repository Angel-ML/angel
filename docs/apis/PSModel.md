# PSModel
PSModel是对矩阵参数和参数服务器客户端的封装，它提供了常见的矩阵元素获取和更新接口。

## 构造方法
- 定义：```def this(ctx: TaskContext, name: String, row: Int, col: Int, blockRow: Int, blockCol: Int)```

- 参数：ctx: TaskContext Angel Task context信息；name: String 矩阵名； row: Int矩阵行数； col: Int矩阵列数； blockRow: Int 每一个矩阵分片包含的行数； blockCol: Int 每一个矩阵分片包含的列数。当在worker中使用PSModel对象时，需要指定ctx参数，即PSModel对象需要与Angel的一个Task绑定，这主要是为了支持BSP和SSP同步模型

## 构造方法
- 定义：```def this(ctx: TaskContext, name: String, row: Int, col: Int)```

- 参数：ctx: TaskContext Angel Task context信息；name: String 矩阵名； row: Int矩阵行数； col: Int矩阵列数。当不指定blockRow和blockCol参数时，表示使用系统自行决定矩阵分区块大小

## getRow
- 定义：```def getRow(rowId: Int): K```


- 功能描述：获取矩阵的某一行。在不同的同步模型下，本方法会有不同的流程。Angel支持3种同步模型：**BSP**，**SSP** 和 **异步** 模型。在**BSP** 和 **SSP** 模型下，本方法会首先检查本地缓存中是否已经存在需要获取的行且该行的时钟信息是否满足同步模型，若缓存中不存在或者不满足同步模型要求，它会向参数服务器请求，如果参数服务器上的行时钟也不满足同步模型，则它会一直等待直到满足为止；在**异步**模型下，该方法会直接向参数服务器请求所需的行，而不关心时钟信息


- 参数：rowId: Int 行号


- 返回值：指定行的行向量

## getRows
- 定义：```def getRows(rowIndexes:Array[Int]): List[K]```


- 功能描述：获取矩阵的某些行。在**BSP/SSP/异步**模型下的获取流程与getRow方法类似


- 参数：rowIndexes:Array[Int] 行号数组


- 返回值：指定行的行向量列表；列表有序，与参数数组顺序一致

## getRowsFlow
- 定义：```def getRowsFlow(rowIndex: RowIndex, batchNum: Int): GetRowsResult```


- 功能描述：以流水化的形式获取矩阵的某些行，该方法会立即返回，用于支持一边计算一边进行行获取，在BSP/SSP/异步模型下的获取流程与getRow方法类似


- 参数：rowIndex: RowIndex 行号集合；batchNum: Int 每次RPC请求的行数，这个参数定义了流水化的粒度，可以设置为-1，表示由系统自行选择


- 返回值：一个行向量队列，上层应用程序可以从该队列中得到已经获取到的行对应的行向量

## increment
- 定义：```def increment(delta: TVector)```


- 功能描述：以累加的方式更新矩阵的某一行，该方法采用异步更新的方式，只是将更新向量缓存到本地，而非直接作用于参数服务器，只有当执行flush或者clock方法时才会将更新作用到参数服务器


- 参数：delta: TVector 与行向量维度一致的更新向量


- 返回值：无

## increment
- 定义：```def increment(deltas: List[TVector])```


- 功能描述：以累加的方式更新矩阵的某些行，该方法采用异步更新的方式，只是将更新向量缓存到本地，而非直接作用于参数服务器，只有当执行flush或者clock方法时才会将更新作用到参数服务器


- 参数：deltas: List[TVector] 与行向量维度一致的更新向量列表


- 返回值：无

## get
- 定义：```def get(func: GetFunc): GetResult```


- 功能描述：使用psf get函数获取矩阵的元素或元素统计信息。与getRow/getRows/getRowsFlow方法不同，本方法只支持异步模型


- 参数：func: GetFunc psf get函数。psf函数是Angel提供的一种参数服务器功能扩展接口，关于psf函数的详细介绍可参考 [psFunc开发手册]()。应用程序可以根据需求自定义psf get函数，当然，Angel提供了一个包含常用函数的函数库 [get函数库]()


- 返回值：GetResult：psf get函数返回结果

## update
- 定义：```def update(func: UpdaterFunc): Future[VoidResult]```


- 功能描述：使用psf update函数更新矩阵的参数。与increment方法不同，本方法会直接将更新作用与参数服务器端


- 参数：func: GetFunc psf update函数。关于psf函数的详细介绍可参考 [psFunc开发手册]()。用户可以根据需求自定义psf update函数，当然，Angel提供了一个包含常用函数的函数库 [update函数库]()。与increment函数不同，本方法会立即将更新作用于参数服务器


- 返回值：Future[VoidResult] psf update函数返回结果，应用程序可以选择是否等待更新结果

## clock
- 定义：```def clock(): Future[VoidResult]```


- 功能描述：将本地缓存的所有矩阵更新（调用increment函数会将更新缓存在本地）合并后发送给参数服务器，然后更新矩阵的时钟


- 参数：无


- 返回值：Future[VoidResult] clock操作结果，应用程序可以选择是否等待clock操作完成

## flush
- 定义：```def flush(): Future[VoidResult]```


- 功能描述：将本地缓存的所有矩阵更新（调用increment函数会将更新缓存在本地）合并后发送给参数服务器


- 参数：无


- 返回值：Future[VoidResult] flush操作结果，应用程序可以选择是否等待flush操作完成

## setAverage
- 定义：```def setAverage(aver: Boolean)```


- 功能描述：设置矩阵更新属性，在更新矩阵时是否先将更新参数除以总的task数量。本属性increment函数中使用，但不影响update函数


- 参数：aver: Boolean true表示在更新矩阵时先将更新参数除以总task数量，false表示不除


- 返回值：无

## setHogwild
- 定义：```def setHogwild(hogwild: Boolean)```


- 功能描述：设置矩阵属性，是否采用**hogwild** 方式存储和更新本地矩阵。当worker task数量大于1时，可以使用**hogwild** 方式节省内存。默认为使用**hogwild** 方式


- 参数：aver: Boolean true表示使用**hogwild** 方式，false表示不使用


- 返回值：无

## setRowType
- 定义：```def setRowType(rowType: MLProtos.RowType)```


- 功能描述：设置矩阵行向量的元素类型和存储方式，可以根据模型特点和稀疏程度来设置该参数。目前Angel支持的矩阵元素类型有int, float和double；存储方式有稀疏和稠密


- 参数：rowType: MLProtos.RowType 目前支持矩阵行向量的元素类型和存储方式有：**T\_DOUBLE\_SPARSE**, **T\_DOUBLE\_DENSE**, **T\_INT\_SPARSE**, **T\_INT\_DENSE**, **T\_FLOAT\_SPARSE**, **T\_FLOAT\_DENSE** 和 **T\_INT\_ARBITRARY**。**T\_DOUBLE\_SPARSE** 表示稀疏double型；**T\_DOUBLE\_DENSE** 表示稠密double型, **T\_INT\_SPARSE** 表示稀疏int型； **T\_INT\_DENSE** 表示稠密int型； **T\_FLOAT\_SPARSE** 表示稀疏float型； **T\_FLOAT\_DENSE** 表示稠密float型；**T\_INT\_ARBITRARY** 表示数据类型为int，但是根据实际情况选择最节省内存的存储方式


- 返回值：无

## setOplogType
- 定义：```def setOplogType(oplogType: String)```


- 功能描述：矩阵更新存储方式，当使用increment方法更新矩阵时，Angel会先将更新行向量缓存在本地。缓存的方式是在本地定义一个和待更新矩阵维度一致的矩阵


- 参数：oplogType: String 目前支持的存储方式有：**DENSE\_DOUBLE**, **DENSE\_INT**, **LIL\_INT** 和 **DENSE\_FLOAT**。**DENSE\_DOUBE** 表示使用一个稠密double型矩阵来存储矩阵的更新，一般当待更新矩阵元素类型为double时选择更新存储方式；**DENSE\_INT** 表示使用一个稠密的int型矩阵来存储矩阵更新，一般当待更新矩阵元素为int类型时选择这种存储方式；**LIL\_INT** 表示使用一个稀疏int型矩阵来存储矩阵更新，当待更新矩阵元素类型为int且更新相对稀疏时选择更新存储方式；**DENSE\_FLOAT** 表示使用一个稠密的float型矩阵来存储矩阵更新，一般当待更新矩阵元素为float类型时选择这种存储方式


- 返回值：无

## setLoadPath
- 定义：```def setLoadPath(path: String)```


- 功能描述：设置矩阵加载路径，本属性用于模型增量更新和预测功能模式下，表示在参数服务器端初始化矩阵时，从文件中读取矩阵参数来初始化


- 参数：path: String 已有的矩阵参数存储路径


- 返回值：无

## setSavePath
- 定义：```def setSavePath(path: String)```


- 功能描述：设置矩阵保存路径；在训练功能模式下，当训练完成时，需要将参数服务器上的矩阵参数保存在文件中


- 参数：path: String 已有的矩阵参数存储路径


- 返回值：无

## setAttribute
- 定义：```def setAttribute(key: String, value: String)```


- 功能描述：设置矩阵参数，Angel可以支持自定义矩阵参数扩展


- 参数：key: String 参数名；value: String参数值


- 返回值：无

## getContext
- 定义：```def getContext(): MatrixContext```


- 功能描述：获取矩阵context信息，context信息包含了所有矩阵的参数和信息


- 返回值：MatrixContext 矩阵context信息


