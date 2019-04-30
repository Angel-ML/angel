# ModelLoader
> After the training process is completed, the model needs to be put into production envirionment. To offer a better interface by which other systems can utilize the models trained by Angel conveniently, Angel provides a `ModelLoader` class as a model loading tool. `ModelLoader` provides a set of static methods that read and store model files in memory in basic data structures (Array and Map).

1. **loadToDoubleArrays**
    - **Definition**: ```double[][] loadToDoubleArrays(String modelDir, Configuration conf) throws IOException```
	- **Functionality**: load a dense `double` type model into a 2-D double array; each row of the model corresponds to a 1-D array
	- **Parameters**: `String modelDir`: model save path; `Configuration conf`: HDFS-related configuration
	- **Return Type**: a 2-D `double` array representing a dense `double` type model

2. **loadToDoubleMaps**
    - **Definition**: ```Int2DoubleOpenHashMap[] loadToDoubleMaps(String modelDir, Configuration conf) throws IOException```
    - **Functionality**: load a sparse `double` type model into an array of `<int, double>` hashmap; each row in the model corresponds to a map
    - **Parameters**: `String modelDir`: model save path; `Configuration conf`: HDFS-related configuration
    - **Return Type**: an array of `<int, double>` map representing a sparse `double` type model

3. **loadToDoubleLongKeyMaps**
    - **Definition**: ```Long2DoubleOpenHashMap[] loadToDoubleLongKeyMaps(String modelDir, Configuration conf) throws IOException```
    - **Functionality**: load a sparse `double` type model (with keys in `long` type) into an array of `<long, double>` hashmap; each row in the model corresponds to a map
    - **Parameters**: `String modelDir`: model save path; `Configuration conf`: HDFS-related configuration
    - **Return Type**: an array of `<long, double>` map representing a sparse `double` type model
    
4. **loadToFloatArrays**
    - **Definition**: ```float[][] loadToFloatArrays(String modelDir, Configuration conf) throws IOException```
	- **Functionality**: load a dense `float` type model into a 2-D `float` array; each row in the model corresponds to a 1-D array
	- **Parameters**: `String modelDir`: model save path; `Configuration conf`: HDFS-related configuration
	- **Return Type**: a 2-D `float` array representing a dense `float` type model

5. **loadToFloatMaps**
    - **Definition**: ```Int2FloatOpenHashMap[] loadToFloatMaps(String modelDir, Configuration conf) throws IOException```
    - **Functionality**: load a sparse `float` type model into an array of `<int, float>` hashmap; each row in the model corresponds to a map
    - **Parameters**: `String modelDir`: model save path; `Configuration conf`: HDFS-related configuration
    - **Return Type**: an array of `<int, float>` map representing a sparse `float` type model

6. **loadToIntArrays**
    - **Definition**: ```int[][] loadToIntArrays(String modelDir, Configuration conf) throws IOException```
    - **Functionality**: load a dense `int` type model into a 2-D `int` array; each row in the model corresponds to a 1-D array
    - **Parameters**: `String modelDir`: model save path; `Configuration conf`: HDFS-related configuration
    - **Return Type**: a 2-D `int` array representing a dense `float` type model

7. **loadToIntMaps**
    - **Definition**: ```Int2IntOpenHashMap[] loadToIntMaps(String modelDir, Configuration conf) throws IOException```
    - **Functionality**: load a sparse `int` type model into an array of `<int, int>` hashmap; each row in the model corresponds to a map
    - **Parameters**: `String modelDir`: model save path; `Configuration conf`: HDFS-related configuration
    - **Return Type**: an array of `<int, int>` map representing a sparse `int` type model