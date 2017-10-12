### ModelConverter（模型转换器）

---

>  Angel任务结束前，PSModel的模型写入，会默认以二进制文件格式存储，这样可以有更好的速度和节省空间。但是对于有些用户，他们希望得到文本格式的模型，ModelConverter类可以将模型文件，从二进制转换成明文，从而让用户更方便的使用。

## 转换前模型文件格式

Angel的模型文件，默认设计如下：

* 每个partition存为一个文件，文件名为partitionID
* 一个PSModel的所有partition保存在同一个文件夹，文件夹的名字为PSModel的modelName字段
 
## 转换命令

提交Angel的ModelConverter任务的命令如下：

```bsh
./bin/angel-submit \
-- action.type train \
-- angel.app.submit.class com.tencent.angel.ml.toolkits.modelconverter.ModelConverterRunner \
-- ml.model.in.path ${modelInPath}
-- ml.model.name ${PSModelName}
-- ml.model.out.path ${modelOutPath} \
-- ml.model.convert.thread.count ${threadCount} \
-- angel.save.model.path ${anywhere} \
-- angel.workergroup.number 1 \
-- angel.worker.memory.mb 1000  \
-- angel.worker.task.number 1 \
-- angel.ps.number 1 \
-- angel.ps.memory.mb 500 \
-- angel.job.name ${jobname}
```

* 参数说明：
    * ml.model.in.path  
      模型输入路径，对应于生成这个任务的"ml.model.out.path"指定的路径
    * ml.model.name lr_weight   
      要解析的模型名字，即：PSModel.modelName
    * ml.model.out.path   
      解析后的明文模型保存路径
    * angel.save.model.path    
      Angel 提交任务时要求一定要配置一个输出路径，这个路径会被清空。这个参数需要填一个路径，但是要注意，会被清空，任务结束后没有输出。
    * angel.workergroup.number   
      模型解析任务默认在一个worker上完成，参数推荐设置为1。
    * angel.worker.task.number 1   \
      模型解析任务默认在一个task上完成，参数推荐设置为1。
    * ml.model.convert.thread.count   
      转换模型的线程数 

### 转换后文件格式说明

* Angel 的模型按照 partition 单位存储，每个 partition 存储为一个文件，名字为这个 partition 的 ID。转换任务输出的模型与 partition 文件一一对应。

* 格式如下，其中第一行的两个值为：rowID、clock值，第二行及后面为模型的 key ：value。

        ```
        0, 10
        0:-0.004235138405748639
        1:-0.003367253227582031
        3:-0.003988846053264014
        6:0.001803243020660425
        8:1.9413353447408782E-4
        ```
        
