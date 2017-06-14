# Angel编译指南

---

1. **编译环境依赖**
    * Jdk >= 1.8
    * Maven >= 3.0.5
    * Protobuf >= 2.5.0

2. **源码下载**

	```git clone https://github.com/tencent/angel```

3. **编译**
    	
	进入源码根目录，执行命令：
    		 
	```mvn clean package -Dmaven.test.skip=true```
    
	编译完成后，在源码根目录`dist/target`目录下会生成一个发布包：`angel-1.1.8-bin.zip`

4. **发布包**

	发布包解压后，根目录下有四个子目录：
   * bin：Angel任务提交脚本
   * conf：系统配置文件
   * data：简单测试数据
   * lib：Angel jar包 & 依赖jar包
