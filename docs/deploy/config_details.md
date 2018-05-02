## 输入输出路径，运行模式，部署模式等相关配置
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
action.type | train | Angel task的运行方式，目前主要有两种“train”和“predict”，分别表示模型训练和使用模型进行预测
angel.train.data.path | 无 | 训练数据所在路径，该选项用于“train”运行方式下
angel.predict.data.path | 无 | 预测数据所在路径，该选项用于“predict”运行方式下
angel.inputformat | org.apache.hadoop.mapreduce.lib<br>.input.CombineTextInputFormat | 训练数据文件格式，主要用于训练数据划分和读取，可支持自定义格式
angel.predict.out.path | 无 | 预测结果存储路径，该选项只用于“predict”运行方式
angel.save.model.path | 无 | 模型存储路径，该选项只用于“train”运行方式
angel.load.model.path | 无 | 模型加载路径，该选项可用于“train”和“predict”运行方式。用于“train”时表示增量学习，即加载旧模型，利用新的训练数据在此基础上得到新的模型；用于“predict”时表示加载该模型进行预测
angel.deploy.mode | YARN | 部署方式，目前支持“YARN”和“LOCAL”两种方式，“LOCAL”部署方式下目前只支持启动一个Worker和PS
angel.running.mode | ANGEL_PS_WORKER | 运行模式，目前支持“ANGEL_PS_WORKER”和“ANGEL_PS”两种模式。“ANGEL_PS_WORKER”表示启动PS和Worker组件，即Angel自己就可以完成整个任务的计算；“ANGEL_PS”表示PS-Service，即只启动PS，向第三方计算平台（例如spark）提供PS服务
angel.job.libjars | 无 | Angel计算任务需要依赖的jar包，多个jar包之间使用逗号分隔
queue | 无 | Angel计算任务使用的资源池，用于Yarn模式下的资源管理
angel.job.name | angel app | Angel计算任务的名字
angel.app.config.file | 无 | Angel支持命令行参数设置和配置文件两种方式，当使用配置文件时，可以上传一个xml格式的参数配置文件 
angel.app.submit.class | com.tencent.angel.utils.DefaultAppSubmitter | Angel任务提交类
angel.task.user.task.class | com.tencent.angel.worker.task.BaseTask | Angel worker运行的task类，用户可以根据需求自定义task类，但需要继承com.tencent.angel.worker.task.BaseTask
angel.staleness | 0 | task之间允许的最大stale值。0表示BSP，大于0表示SSP，小于0表示ASYNC
    

## 资源与运行环境相关配置
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
angel.am.env | 无 | Angel Master环境变量设置，形式为K1=V1,K2=V2
angel.worker.env | 无 | Angel Worker环境变量设置，形式为K1=V1,K2=V2
angel.ps.env | 无 | Angel PS环境变量设置，形式为K1=V1,K2=V2
angel.workergroup.number | 1 | 需要启动的workergroup数量，目前一个workergroup仅支持一个worker，因此worker和workergroup数量相等
angel.worker.task.number | 1 | 每个worker上运行的task数量
angel.ps.number | 1 | 需要启动的ps数量
angel.am.cpu.vcores | 1 | Angel master使用的CPU vcore数量
angel.am.memory.mb | 2048 | Angel master使用的内存大小，以MB为单位
angel.am.memory.gb | 2 | Angel master使用的内存大小，以GB为单位
angel.am.java.opts | 无 | Angel master进程JVM参数
angel.worker.cpu.vcores | 1 | 一个worker使用的vcore数量
angel.worker.memory.mb | 4096 | 一个worker使用的内存大小，以MB为单位
angel.worker.memory.gb | 4 | 一个worker使用的内存大小，以GB为单位
angel.worker.java.opts | 无 | worker进程JVM参数
angel.ps.cpu.vcores | 1 | 一个PS使用的vcore数量
angel.ps.memory.mb | 4096 | 一个PS使用的内存大小，以MB为单位
angel.ps.memory.mb | 4 | 一个PS使用的内存大小，以GB为单位
angel.ps.java.opts | 无 | PS进程JVM参数


## RPC相关配置
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
angel.am.heartbeat.interval.ms | 5000 | Angel master向Yarn RM发送心跳的时间间隔，单位为毫秒
angel.worker.heartbeat.interval.ms | 5000 | Angel worker向Angel master发送心跳的时间间隔，单位为毫秒
angel.ps.heartbeat.interval.ms | 5000 | Angel PS向Angel master发送心跳的时间间隔，单位为毫秒
angel.netty.matrixtransfer.client.sndbuf | 1048576 | Netty矩阵传输客户端使用的发送缓冲区的大小，单位为字节。PSAgent和PS之间的矩阵数据传输使用的网络框架是Netty，PSAgent为客户端，所以Netty客户端位于PSAgent侧。
angel.netty.matrixtransfer.client.rcvbuf | 1048576 | Netty矩阵传输客户端使用的接收缓冲区的大小，单位为字节。
angel.netty.matrixtransfer.client.eventgroup.threadnum | 24 | Netty矩阵传输客户端使用的工作线程数量（NioEventLoopGroup使用的线程数）。
angel.netty.matrixtransfer.client.usedirectbuffer | true | Netty矩阵传输客户端是否要使用direct buffer。true表示要使用direct buffer，false表示使用普通堆内存。
angel.netty.matrixtransfer.server.sndbuf | 1048576 | Netty矩阵传输服务端使用的发送缓冲区的大小，单位为字节。PSAgent和PS之间的矩阵数据传输使用的网络框架是Netty，PS为服务端。
angel.netty.matrixtransfer.server.rcvbuf | 1048576 | Netty矩阵传输服务端使用的接收缓冲区的大小，单位为字节。
angel.netty.matrixtransfer.server.eventgroup.threadnum | 24 | Netty矩阵传输服务端使用的工作线程数量（NioEventLoopGroup使用的线程数）。
angel.netty.matrixtransfer.server.usedirectbuffer | true | Netty矩阵传输服务端是否要使用direct buffer。true表示要使用direct buffer，false表示使用普通堆内存。
angel.netty.matrixtransfer.max.message.size | 104857600 | Netty矩阵传输支持的最大的单个消息的大小，单位为字节，默认为100M。
angel.matrixtransfer.max.requestnum.perserver | 4 | PSAgent能够最多同时向一个PS发起多少个请求，该参数用于流量控制，避免同时向一个PS发起太多请求而导致出流量占满，进而导致发往其他PS的请求等待。
angel.matrixtransfer.max.requestnum | 64 | PSAgent能够最多同时向所有PS发起多少个请求，该参数用于流量控制。

## 系统可用性相关配置
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
angel.am.max-attempts | 2 | Master最大运行尝试次数。
angel.worker.max-attempts | 4 | Worker最大运行尝试次数。
angel.ps.max-attempts | 4 | PS最大运行尝试次数。
angel.am.appstate.timeout.ms | 30000 | Application处于临时状态最大允许时间，单位为毫秒。
angel.am.wait.matrix.timeout.ms | 30000 | Master等待客户端提交矩阵配置信息超时时间，单位为毫秒。
angel.am.appstate.timeout.ms | 30000 | Application处于临时状态最大允许时间，单位为毫秒。临时状态指的是除了RUNNING和COMMITTING外的其他状态，如果处于临时状态的时间超过了限制，Application将会退出
angel.am.write.state.interval.ms | 10000 | Master将作业状态信息写入hdfs时间间隔，单位为毫秒。
angel.worker.heartbeat.interval.ms | 5000 | Worker向Master发送的心跳间隔时间，单位为毫秒。
angel.worker.heartbeat.timeout.ms | 60000 | Worker向Master发送的心跳超时时间，单位为毫秒。如果一个Worker超过该时间没有向Master上报心跳，将会被Master判定为异常。
angel.ps.heartbeat.interval.ms | 5000 | PS向Master发送的心跳间隔时间，单位为毫秒。
angel.ps.heartbeat.timeout.ms | 60000 | PS向Master发送的心跳超时时间，单位为毫秒。如果一个PS超过该时间没有向Master上报心跳，将会被Master判定为异常。
angel.ps.backup.interval.ms | 60000 | PS将所承载的参数写入hdfs间隔时间，单位为毫秒。PS会每隔一段时间将存储在其上的矩阵参数写入hdfs，用在PS宕机恢复。
angel.matrixtransfer.retry.interval.ms | 3000 | 失败的矩阵传输请求重试时间间隔，单位为毫秒。
angel.matrixtransfer.request.timeout.ms | 30000 | 矩阵传输请求超时时间，单位为毫秒。 

## 日志相关配置
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
angel.am.log.level | INFO | Master日志输出级别，可配置的有DEBUG,INFO,WARN和ERROR
angel.worker.log.level | INFO | Worker日志输出级别，可配置的有DEBUG,INFO,WARN和ERROR
angel.ps.log.level | INFO | PS日志输出级别，可配置的有DEBUG,INFO,WARN和ERROR

## 缓存相关配置
    
配置项名称 | 默认值 | 配置项含义
---------------- | --------------- | ---------------
angel.psagent.cache.sync.timeinterval.ms | 50 | PSAgent侧缓存更新时间间隔，单位为毫秒。为了实现矩阵参数预取功能，在PSAgent侧维护了一个矩阵参数的缓存，该缓存每隔一段时间和PS侧数据进行同步。
angel.psagent.sync.policy.class | com.tencent.angel.psagent.matrix<br>.cache.DefalutPolicy | PSAgent侧缓存更新策略。
angel.task.data.storage.level | memory_disk | Task预处理过的训练数据存储方式，可选的有memory,memory_disk和disk。memory表示将所有训练数据存储在内存中，如果内存足够大，建议使用这种方式；disk表示将所有训练数据存储在本地磁盘上；memory_disk则介于两者之间，可以支持一部分放在内存中，其余的放在本地磁盘。
angel.task.memorystorage.max.mb | 1000 | 每一个task运行使用多大内存来存放训练数据，单位为MB。只有当storage level配置为memory_disk时该选项才起作用。

