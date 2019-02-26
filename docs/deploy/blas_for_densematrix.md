# 使用Blas给算法加速

---
深度学习中存在大量的稠密矩阵运算的乘法，稠密矩阵乘法是典型的计算密集型任务，如果使用纯java来实现稠密矩阵乘法，效果往往不如人意。Angel数学库支持用Blas库来加速稠密矩阵乘法，当然，Angel没有直接通过JNI来调用Blas相关接口，而是使用了[netlib][1]封装。默认情况下，netlib会首先尝试加载计算节点上的Blas本地动态链接库，如果加载失败，则会退回到java版本的实现。因此，如果要使用Blas库来加速需要完成下面的准备工作。

## 编译OpenBlas
### 下载最新的OpenBlas源码包
下载地址：https://github.com/xianyi/OpenBLAS/releases

### 编译得到libopenblas.so
上传到计算节点或者与计算节点环境相同的机器上，解压，编译后得到**libopenblas.so**。编译方式可以参考OpenBlas官方文档：https://github.com/xianyi/OpenBLAS/wiki/User-Manual

## 使用OpenBlas
### 提交任务的时候上传OpenBlas库和netlib相关的库
将libopenblas.so和netlib相关包放入任务上传文件列表
打开conf目录下的angel-site.xml配置文件，将 *"${ANGEL_HOME}/lib/jniloader-1.1.jar,${ANGEL_HOME}/lib/native_system-java-1.1.jar,${ANGEL_HOME}/lib/libopenblas.so,${ANGEL_HOME}/lib/arpack_combined_all-0.1.jar,${ANGEL_HOME}/lib/core-1.1.2.jar,${ANGEL_HOME}/lib/netlib-native_ref-linux-armhf-1.1-natives.jar,${ANGEL_HOME}/lib/netlib-native_ref-linux-i686-1.1-natives.jar,${ANGEL_HOME}/lib/netlib-native_ref-linux-x86_64-1.1-natives.jar,${ANGEL_HOME}/lib/netlib-native_system-linux-armhf-1.1-natives.jar,${ANGEL_HOME}/lib/netlib-native_system-linux-i686-1.1-natives.jar,${ANGEL_HOME}/lib/netlib-native_system-linux-x86_64-1.1-natives.jar"* 加入**angel.job.libjar**选项的头部

### 配置worker和ps环境变量
使用**LD_PRELOAD**环境变量来通知进程加载OpenBlas动态链接库：`LD_PRELOAD=./libopenblas.so`

OpenBlas支持多线程加速，所以你可以根据机器的状态和矩阵规模适当调节并发度的大小。调整方式为设置环境变量**OPENBLAS_NUM_THREADS**，例如将并发度设置为8：
`OPENBLAS_NUM_THREADS=8`

由于计算主要发生在Worker和PS上，所以只需要配置Worker和PS的环境变量即可：
`angel.worker.env="LD_PRELOAD=./libopenblas.so,OPENBLAS_NUM_THREADS=8"
angel.ps.env="LD_PRELOAD=./libopenblas.so,OPENBLAS_NUM_THREADS=8"`


### 如何确认OpenBlas已经生效了
打开worker和ps的syserr日志输出，没有出现"Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS"错误信息即表示OpenBlas已经生效了


  [1]: https://github.com/fommil/netlib-java