## Available Properties of I/O Path, Running Mode, Deploy Mode
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
action.type | train | Angel task type; supports "train" for model training and "predict" for generating predictions from model
angel.train.data.path | (none) | Path of data for training , used when action.type is "train"
angel.predict.data.path | (none) | Path of data for prediction , used when action.type is "predict"
angel.inputformat | org.apache.hadoop.mapreduce.lib<br>.input.CombineTextInputFormat | Format of the training data, mainly used for data partitioning and reading, supporting customized formats
angel.predict.out.path | (none) | Save path for "predict" result, only used when action.type is "predict"
angel.save.model.path | (none) | Save path for the model, only used when action.type is "train"
angel.load.model.path | (none) | Path for loading model, can be used for both "train" and "predict" types. When action.type is "train", it loads the old model and does incremental learning. When action.type is "predict", it loads the model for prediction. 
angel.deploy.mode | YARN | Deploy mode, currently supports "YARN" and "LOCAL". Currently, "LOCAL" mode only supports starting up one worker and one PS
angel.running.mode | ANGEL_PS_WORKER | Running mode, currently supports "ANGEL_PS_WORKER" and "ANGEL_PS" modes. "ANGEL_PS_WORKER" starts up PS and worker components, thus Angel can complete the computation of the entire job. "ANGEL_PS" starts up PS only, providing PS-service to third-party frameworks (such as spark)
angel.job.libjars | (none) | Jars that the Angel application depends on. Use `,` to separate multiple jars. 
queue | (none) | Resource pool used by the Angel application, used for resource management under Yarn mode. 
angel.job.name | angel app | Name of Angel application
angel.app.config.file | (none) | Config file. You can use either command-line or config file to configure Angel's parameters. You can upload a xml format config file. 
angel.app.submit.class | com.tencent.angel.utils.DefaultAppSubmitter | Class for Angel application submission
angel.task.user.task.class | com.tencent.angel.worker.task.BaseTask | Task class that Angel worker runs; can be customized as long as it inherits com.tencent.angel.worker.task.BaseTask
angel.staleness | 0 | Maximum staleness among tasks: 0 means BSP, >0 means SSP, <0 means ASYNC
    

## Available Properties of Resource and Runtime Environment 
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
angel.am.env | (none) | Configures Angel master environment, in the form of Key1=Val1, Key2=Val2, ...
angel.worker.env | (none) | Configures Angel worker environment, in the form of key1=Val1, Key2=Val2, ...
angel.ps.env | (none) | Configures Angel PS environment, in the form of Key1=Val1, Key2=Val2, ...
angel.workergroup.number | 1 | Number of workergroups to be started. Currently, one workergroup supports one worker only (therefore there are equal numbers of workers and workergroups)
angel.worker.task.number | 1 | Number of tasks run on each worker
angel.ps.number | 1 | Number of PS to be started
angel.am.java.opts | (none) | JVM parameters for Angel master process
angel.am.resource.cpu-vcores | 1 | Number of CPU vcores used by Angel master
angel.worker.memory.mb | 1024 | Memory used by a single worker (MB)
angel.worker.java.opts | (none) | JVM parameters for worker process
angel.worker.cpu.vcores | 1 | Number of vcores used by a single worker
angel.ps.memory.mb | 1024 | Memory used by a single PS (MB)
angel.ps.java.opts | (none) | JVM parameters for PS process
angel.ps.cpu.vcores | 1 | Number of vcores used by a single PS

## Available Properties of RPC
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
angel.am.heartbeat.interval.ms | 5000 | Interval (ms) for heartbeats sent from Angel master to Yarn RM to prevent connection timeout
angel.worker.heartbeat.interval.ms | 5000 | Interval (ms) for heartbeats sent from Angel worker to Angel master
angel.ps.heartbeat.interval.ms | 5000 | Interval (ms) for heartbeats sent from Angel PS to Angel master
angel.netty.matrixtransfer.client.sndbuf | 1048576 | Send buffer size (byte) used by Netty client for transferring matrix data. We use Netty framework for matrix data transfer between PSAgent and PS; PSAgent is the client, thus Netty client is on the PSAgent side 
angel.netty.matrixtransfer.client.rcvbuf | 1048576 | Receive buffer size (byte) used by Netty client
angel.netty.matrixtransfer.client.eventgroup.threadnum | 24 | Number of threads on Netty client (number of threads used by NioEventLoopGroup)
angel.netty.matrixtransfer.client.usedirectbuffer | true | If true, Netty client will use direct buffer; if false, Netty client will use heap
angel.netty.matrixtransfer.server.sndbuf | 1048576 | Send buffer size (byte) of Netty server. We use Netty framework for matrix data transfer between PSAgent and PS; PS is the server
angel.netty.matrixtransfer.server.rcvbuf | 1048576 | Receive buffer size (byte) of Netty server 
angel.netty.matrixtransfer.server.eventgroup.threadnum | 24 | Number of threads on Netty server (number of threads used by NioEventLoopGroup)
angel.netty.matrixtransfer.server.usedirectbuffer | true | If true, Netty server will use direct buffer; if false, Netty server will use heap
angel.netty.matrixtransfer.max.message.size | 104857600 | Maximum message size (byte) supported by Netty matrix transfer
angel.matrixtransfer.max.requestnum.perserver | 4 | Maximum number of simultaneous requests that PSAgent sends to any PS, used for preventing data overflow caused by too many simultaneous requests to a PS that usually delays requests to other PS
angel.matrixtransfer.max.requestnum | 64 | Maximum number of simultaneous requests that PSAgent sends to all PS, used for flow control

## Available Properties for System Availability
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
angel.am.max-attempts | 2 | Maximum number of attempts of the master
angel.worker.max-attempts | 4 | Maximum number of attempts of the worker
angel.ps.max-attempts | 4 | Maximum number of attempts of the PS
angel.am.appstate.timeout.ms | 30000 | How long (ms) the application can be in a state that is neither RUNNING nor COMMITTING before it quits
angel.am.wait.matrix.timeout.ms | 30000 | Timeout (ms) for the master to wait for matrix configuration submitted by the client 
angel.am.write.state.interval.ms | 10000 | Timeout (ms) for the master to write job status onto hdfs
angel.worker.heartbeat.interval.ms | 5000 | Interval (ms) for heartbeats sent from worker to master
angel.worker.heartbeat.timeout.ms | 60000 | How long (ms) the master will wait for heartbeat sent from worker before timing out
angel.ps.heartbeat.interval.ms | 5000 | Interval (ms) for heartbeats sent from PS to master
angel.ps.heartbeat.timeout.ms | 60000 | How long (ms) the master will wait for heartbeat sent from PS before timing out 
angel.ps.backup.interval.ms | 60000 | Interval (ms) for PS to write loaded parameters onto hdfs. PS does so regularly in order to be able to recover after shutdown
angel.matrixtransfer.retry.interval.ms | 3000 | Interval (ms) for retries of failed matrix transfer 
angel.matrixtransfer.request.timeout.ms | 30000 | Timeout (ms) for matrix transfer 

## Available Properties for Log
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
angel.am.log.level | INFO | Master log output level: DEBUG, INFO, WARN, ERROR 
angel.worker.log.level | INFO | Worker log output level: DEBUG, INFO, WARN, ERROR
angel.ps.log.level | INFO | PS log output level: DEBUG, INFO, WARN, ERROR

## Available Properties for Cache
    
Property Name | Default | Meaning
---------------- | --------------- | ---------------
angel.psagent.cache.sync.timeinterval.ms | 50 | Interval (ms) for cache updating on the PSAgent side. To prefetch the matrix parameters, we maintain a cache for the matrix parameters on the PSAgent side, which synchronize with data on the PS side regularly
angel.psagent.sync.policy.class | com.tencent.angel.psagent.matrix<br>.cache.DefalutPolicy | Cache policy on the PSAgent side
angel.task.data.storage.level | memory_disk | Storage format for training data preprocessed by task, supporting `memory`, `memory_disk` and `disk`. `memory` means storing all training data in the memory, recommended if the memory is large enough. `disk` means storing all training data on local disk. `memory_disk` supports allocating storage to both the memory and disk. 
angel.task.memorystorage.max.mb | 1000 | Memory size (MB) used by each task for storing training data, only used when storage level is set to `memory_disk`
