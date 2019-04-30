# 模型存储格式
Angel的模型是以矩阵为单位来保存的，每一个矩阵在模型保存路径下对应一个以矩阵名命名的文件夹，里面包含矩阵的元数据文件和数据文件。一个矩阵只有一个元数据文件，但是一般有多个数据文件，因为Angel的大部分算法模型都是从PS导出的。

## 元数据文件

元数据采用JSON格式保存。矩阵元数据主要由矩阵特征，分区索引和行相关索引组成：分别由**MatrixFilesMeta**，**MatrixPartitionMeta**和**RowPartitionMeta**类来描述。
### MatrixFilesMeta
**用途**：矩阵相关信息
**具体包含字段**：

 - **matrixId**：矩阵ID 
 - **rowType**：矩阵类型，可参看RowType类
 - **row**：矩阵行数 
 - **blockRow**：矩阵分区块行数
 - **col**：矩阵列数 
 - **blockCol**：矩阵分块块列数 
 - **matrixName**：矩阵名字 
 - **formatClassName**：矩阵存储格式
 - **options**：其他矩阵参数 
 - **partMetas**：矩阵分区索引

### MatrixPartitionMeta
**用途**：分区元数据
**具体包含字段**：
- **startRow**：分区起始行行号
- **endRow**：分区结束行行号
- **startCol**：分区起始列列号
- **endCol**：分区结束列列号
- **nnz**：分区非零元素个数（暂时没用）
- **fileName**：分区数据所在文件名
- **offset**：分区数据在文件中的位置
- **length**：分区数据长度（字节数）
- **saveRowNum**：分区中保存的行数
- **saveColNum**：分区中保存的列数（只在列主序格式中有用）
- **saveColElemNum**：每一列保存的元素个数（只在列主序格式中有用）
- **rowMetas**：分区行索引

### RowPartitionMeta
**用途**：分区的某一行分片对应的元数据
**具体包含字段**：
- **rowId**：行号
- **offset**：行数据在文件中的位置
- **elementNum**：该行包含的元素个数
- **saveType**：该行保存的文件格式

## 数据文件格式
Angel2.0采用了用户自定义的模型格式。即可以根据实际需求定制模型输出格式。一般情况下，使用Angel的默认模型输出格式即可。由于Angel默认的输出格式比较简单，大部分并不需要依赖元数据文件就可以直接解析。


### 默认格式说明
Angel提供了8中默认的模型输出格式：
**ValueBinaryRowFormat**，**ColIdValueBinaryRowFormat**，**RowIdColIdValueBinaryRowFormat**，**ValueTextRowFormat**，**ColIdValueTextRowFormat**，**RowIdColIdValueTextRowFormat**，**BinaryColumnFormat和TextColumnFormat**。 下面分别介绍这8种格式。
#### ValueBinaryRowFormat
- **说明**：二进制格式，只包含模型的值。这种格式只适合单行稠密的模型，由于数据文件中没有列号（特征索引），所以需要从模型元数据中获取索引范围。
- **格式**：|value|value|...|

####  ColIdValueBinaryRowFormat
- **说明**：二进制格式，包含特征索引和对应的值。这种格式适合单行模型，例如LR等。
- **格式**：|index|value|index|value|...|

#### RowIdColIdValueBinaryRowFormat
- **说明**：二进制格式，包含模型的行号，特征索引和对应的值。这种格式可以表示多行模型。
- **格式**：|rowid|index|value|rowid|index|value|...|

#### ValueTextRowFormat
- **说明**：文本格式，只包含模型的值，每个值是一个单独的行。与ValueBinaryRowFormat类似这种格式只适合单行稠密的模型，由于数据文件中没有列号（特征索引），所以需要充模型元数据中获取索引范围。
- **格式**：<br>
value<br>
value<br>
value

#### ColIdValueTextRowFormat
- **说明**：文本格式，包含特征索引和对应的值，每一行是一个特征id和值的对，特征id和值之间的分隔符默认是逗号。这种格式适合单行模型，例如LR等。
- **格式**：<br>
index,value<br>
index,value<br>
index,value

#### RowIdColIdValueTextRowFormat
- **说明**：文本格式，包含行号，特征索引和对应的值，每一行是一个行号，特征id和值的对。行号，特征id和值之间的分隔符默认是逗号。这种格式可以表示多行模型。
- **格式**：<br>
rowid,index,value<br>
rowid,index,value<br>
rowid,index,value

#### BinaryColumnFormat
- **说明**：二进制格式，这种格式是以列主序来输出一个矩阵，目前只用于Embedding相关的输出（例如DNN，FM等算法中的Embedding层）。模型格式为
- **格式**：|index|row1 value|row2 value|...|index|row1 value|row2 value|...|

#### TextColumnFormat
- **说明**：文本格式，这种格式是以列主序来输出一个矩阵，目前只用于Embedding相关的输出（例如DNN，FM等算法中的Embedding层），每一个行是一个列。分隔符默认是逗号。
- **格式**：<br>
index,row1 value,row2 value,...<br>
index,row1 value,row2 value,...<br>
index,row1 value,row2 value,...

## 具体算法输出格式
Angel的算法目前基本都是基于新的计算图框架来实现的，计算图中的每一层都可以单独设置模型格式。**在默认的情况下，SimpleInputLayer使用的是ColIdValueTextRowFormat，Embedding层使用的是TextColumnFormat，FCLayer使用的是RowIdColIdValueTextRowFormat。**

- **LR，线性回归，SVM**：默认的模型保存格式为ColIdValueTextRowFormat。
- **GBDT**：RowIdColIdValueTextRowFormat
- **FM**：线性部分使用的是ColIdValueTextRowFormat，Embedding层使用的是TextColumnFormat
- **DeepFM, DNN，Wide And Deep，PNN，NFM等**：线性部分使用的是ColIdValueTextRowFormat，Embedding层使用的是TextColumnFormat，全连接部分使用的是RowIdColIdValueTextRowFormat

当然，如果你不想使用默认格式，可以通过参数配置模型输出格式： 

- ***ml.simpleinputlayer.matrix.output.format***：SimpleInputLayer使用的输出格式
- ***ml.embedding.matrix.output.format***：Embedding使用的输出格式
- ***ml.fclayer.matrix.output.format***：FCLayer使用的输出格式

更进一步，如果Angel提供8种格式无法满足你的要求，你也可以扩展RowFormat类或者ColumnFormat类来自定义需要的格式。具体实现方式很简单，你可以参考目前已有的8种类型。实现完成后，编译打包并通过Angel提供的参数加入Angel的依赖路径，同时通过上面提到的四个参数进行配置来使用自定义的输出格式。


