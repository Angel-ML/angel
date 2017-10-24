# ModelLoader（模型加载）
> 在模型训练完成后需要将模型投入生产环境。为了更好的与其他系统对接，让其他系统更好的使用Angel训练好的模型，Angel提供了一个模型加载工具， 即ModelLoader类。ModelLoader类提供了一批静态方法，这些方法将模型文件读入内存中，存储在基本的数据结构（数组和Map中）。

1. **loadToDoubleArrays**
    - 定义：```double[][] loadToDoubleArrays(String modelDir, Configuration conf) throws IOException```
	- 功能描述：将稠密double类型的模型加载到一个double二维数组中，模型的一行对应一个一维数组
	- 参数：String modelDir 模型保存路径；Configuration conf hdfs相关配置
	- 返回值：使用二维double数组表示的稠密double类型的模型

2. **loadToDoubleMaps**
    - 定义：```Int2DoubleOpenHashMap[] loadToDoubleMaps(String modelDir, Configuration conf) throws IOException```
	- 功能描述：将稀疏double类型的模型加载到一个<int, double>映射的数组中，模型的一行对应一个map
	- 参数：String modelDir 模型保存路径；Configuration conf hdfs相关配置
	- 返回值：使用一个<int, double>映射的数组表示的稀疏double类型的模型

3. **loadToDoubleLongKeyMaps**
    - 定义：```Long2DoubleOpenHashMap[] loadToDoubleLongKeyMaps(String modelDir, Configuration conf) throws IOException```
	- 功能描述：将稀疏double类型（拥有long类型的key）的模型加载到一个<long, double>映射的数组中，模型的一行对应一个map
	- 参数：String modelDir 模型保存路径；Configuration conf hdfs相关配置
	- 返回值：使用一个<long, double>映射的数组表示的稀疏double类型的模型
	

4. **loadToFloatArrays**
    - 定义：```float[][] loadToFloatArrays(String modelDir, Configuration conf) throws IOException```
	- 功能描述：将稠密float类型的模型加载到一个float二维数组中，模型的一行对应一个一维数组
	- 参数：String modelDir 模型保存路径；Configuration conf hdfs相关配置
	- 返回值：使用二维float数组表示的稠密float类型的模型

5. **loadToFloatMaps**
    - 定义：```Int2FloatOpenHashMap[] loadToFloatMaps(String modelDir, Configuration conf) throws IOException```
	- 功能描述：将稀疏float类型的模型加载到一个<int, float>映射的数组中，模型的一行对应一个map
	- 参数：String modelDir 模型保存路径；Configuration conf hdfs相关配置
	- 返回值：使用一个<int, float>映射的数组表示的稀疏float类型的模型

6. **loadToIntArrays**
    - 定义：```int[][] loadToIntArrays(String modelDir, Configuration conf) throws IOException```
	- 功能描述：将稠密int类型的模型加载到一个int二维数组中，模型的一行对应一个一维数组
	- 参数：String modelDir 模型保存路径；Configuration conf hdfs相关配置
	- 返回值：使用二维int数组表示的稠密int类型的模型

7. **loadToIntMaps**
    - 定义：```Int2IntOpenHashMap[] loadToIntMaps(String modelDir, Configuration conf) throws IOException```
	- 功能描述：将稀疏int类型的模型加载到一个<int, int>映射的数组中，模型的一行对应一个map
	- 参数：String modelDir 模型保存路径；Configuration conf hdfs相关配置
	- 返回值：使用一个<int, int>映射的数组表示的稀疏int类型的模型

