set bin=%~dp0
set lib=%bin%..\lib
set HADOOP_HOME=%bin%..

java -cp %lib%\* %*