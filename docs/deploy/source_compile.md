# Angel编译指南

---

1. **源码下载**

	```git clone https://github.com/tencent/angel```

2. **编译**

    2.1. **手动编译**
    
     2.1.1. **编译环境依赖**
        
            * Jdk >= 1.8
            * Maven >= 3.0.5
            * Protobuf >= 2.5.0
                * 需与hadoop环境自带的protobuf版本保持一致。目前hadoop官方发布包使用的是2.5.0版本，所以推荐使用2.5.0版本，除非你自己使用更新的protobuf版本编译了hadoop

     2.1.2. **编译命令**
        
            进入源码根目录，执行命令：
        
            ```mvn clean package -Dmaven.test.skip=true```
        
            编译完成后，在源码根目录`dist/target`目录下会生成一个发布包：`angel-${version}-bin.zip`
            
    2.2. **使用docker编译**
    
            如果有安装docker环境, 直接执行根目录下的`docker-build.sh`脚本即可编译，不需要手动安装依赖; 编译完成后`dist`目录下会生成发布包：`angel-${version}-bin.zip`

3. **发布包**

	发布包解压后，根目录下有几个子目录：

	* bin：	Angel任务提交脚本
	* conf：	系统配置文件
	* data：	简单测试数据
	* lib：	Angel的jar包 & 依赖jar包
	* python: Python相关脚本
