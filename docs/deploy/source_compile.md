# Angel编译指南

---

1. **源码下载**

	```git clone https://github.com/tencent/angel```

2. **编译**

    2.1. **手动编译**

     2.1.1. **编译环境依赖**

    * JDK 1.8。
    * Maven 3.x，本次验证使用3.9.6。
    * Protobuf 2.5.0，`protoc`需在`PATH`中，并与工程的`protobuf-java`及Hadoop依赖版本匹配。
    * 编译依赖版本：Scala 2.12.15、Spark 3.3.1、Hadoop 2.7.3；运行已在Spark 3.3.4和Hadoop 2.10.2上验证。

     2.1.2. **编译命令**

    进入源码根目录，执行：

    ```bash
    mvn clean package -Dmaven.test.skip=true
    ```

    编译完成后，在`dist/target`生成`angel-${version}-bin.zip`。该命令跳过测试，构建成功不代表所有算法已验证。

    2.2. **使用docker编译**

            如果有安装docker环境, 直接执行根目录下的`docker-build.sh`脚本即可编译，不需要手动安装依赖; 编译完成后`dist`目录下会生成发布包：`angel-${version}-bin.zip`

3. **发布包**

	发布包解压后，根目录下有几个子目录：

	* bin：	任务提交脚本
		* `spark-on-angel-env.sh`、`SONA-example`：Spark on Angel 环境配置与示例提交脚本
		* `angel-cluster-up`、`angel-standalone-submit`、`angel-ps-launch`（前两者另有Windows下的`.bat`版本）：STANDALONE模式的集群启动/提交脚本，见[STANDALONE / EMBEDDED / Attach 部署](standalone_deploy.md)
	* conf：	系统配置文件
	* data：	简单测试数据
	* jsons：	Angel算法的json配置示例
	* lib：	Angel的jar包 & 依赖jar包

    STANDALONE虽无需运行Yarn服务，仍需Hadoop Java依赖。当前发布包中的Hadoop 2.x jar与`hadoop-client-api`、`hadoop-client-runtime`可能冲突；本地直接运行和独立PS启动时需使用无冲突的classpath，见[本地运行](local_run.md)与[其他部署方式](standalone_deploy.md)。
