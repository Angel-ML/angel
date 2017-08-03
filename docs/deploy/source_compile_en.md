# Angel Compilation Guide

---

1. **Compiling Environment Dependencies**
    * Jdk >= 1.8
    * Maven >= 3.0.5
    * Protobuf >= 2.5.0 should be consistent with the protobuf version that Hadoop is built with; in current Apache Hadoop release, it is 2.5.0, so we recommend using version 2.5.0 unless you have compiled Hadoop using a different version of protobuf. 

2. **Source Code Download**

	```git clone https://github.com/Tencent/angel```

3. **Compile**

    Run the following command in the root directory of the source code:

	```mvn clean package -Dmaven.test.skip=true```

    After compiling, a distribution package named 'angel-&lt;version&gt;-bin.zip'  will be generated under `dist/target` under the root directory. 



4. **Distribution Package**

    Unpacking the distribution package, four subdirectories will be generated under the root directory: 
    * bin: contains Angel submit scripts
    * conf: contains system config files
    * data: contains data for testing
    * lib: contains jars for Angel and dependencies
