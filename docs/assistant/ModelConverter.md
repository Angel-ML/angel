### ModelConverter（模型转换器）

---

>  Angel任务结束前，PSModel的模型写入，会默认以二进制文件格式存储，这样可以有更好的速度和节省空间。但是对于有些用户，他们希望得到文本格式的模型，ModelConverter类可以将模型文件，从二进制转换成明文，从而让用户更方便的使用。

## 转换前模型文件格式

Angel的模型文件，默认设计如下：

* 每个模型拥有一个源文件"meta"，保存了这个模型的参数和分区（partition）信息，以及每个分区数据的索引信息
* 多个模型文件，每个模型文件保存了一个或者多个模型分区的数据
 
## 转换命令

提交Angel的ModelConverter任务的命令如下：

```bsh
./bin/angel-model-convert \
--angel.load.model.path ${anywhere} \
--angel.save.model.path ${anywhere} \
--angel.modelconverts.model.names ${models} \
--angel.modelconverts.serde.class ${SerdeClass}
```

* 参数说明：
    * angel.load.model.path  
      模型输入路径，即原始的二进制格式的模型保存路径。注意，这个路径不用带矩阵名
    * angel.save.model.path   
      转换后文本格式的模型存储路径
    * angel.modelconverts.model.names   
      需要转换的模型名称列表，可以指定多个模型名，用“,”分隔。**这个参数不是必须的，当不指定这个参数时，会转换指定输入路径下所有的模型**
    * angel.modelconverts.serde.class    
      模型输出行序列化格式。**这个参数不是必须的，当不指定这个参数时，使用默认的行格式，即“index:value”形式**

### 转换后文件格式说明

* 模型按照分区为单位存储，一个输出文件可能包含一个或多个分区， 输出文件和partition的对应关系与原始二进制格式相同。

* 格式如下，在每一行的起始处行号标识“rowIndex=xxx”，随后为一系列的key:value，key和value分别表示该行下的列索引和对应的值。**注意一行可能被分在多个分区下，因而完整的一行可能被分成多段存储在不同的文件中**。

        ```
        rowIndex=0
        0:-0.004235138405748639
        1:-0.003367253227582031
        3:-0.003988846053264014
        6:0.001803243020660425
        8:1.9413353447408782E-4
        ```
        
