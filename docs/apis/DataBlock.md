# DataBlock\<VALUE\>

---

> DataBlock是数据块管理和存储基类，它提供了基本的数据存取接口，非常适合一次写入，多次读取的场景（例如训练数据集）

## 功能

DataBlock可以看作是一个可以可以动态增长的"**数组**"，新加入的对象只能放置在数组的末尾， 它在内部维护了读写索引信息。根据存储介质的不同，Angel实现了3种基本的接口实现：

 - **MemoryDataBlock**： 数据存于内存中，可以支持随机访问和顺序访问
 - **DiskDataBlock**： 数据存于本地磁盘中，目前仅支持顺序访问
 - **MemoryAndDiskDataBlock**： 尽量将数据放在内存中，内存放不下时，将数据写入本地磁盘

## 核心接口

1. **registerType**
	- 定义：```void registerType(Class<VALUE> valueClass)```
	- 功能描述：设置存储对象类型
	- 参数
		- Class<VALUE> valueClass 存储对象类型
	- 返回值：无

2. **read**

	- 定义：```VALUE read()```
	- 功能描述：将读索引加1，然后读取读索引位置的对象，如果已经读到末尾，直接返回null
	- 参数：无
	- 返回值：下一个对象，若返回值为null，表明已经读到末尾

8. **loopingRead**
	- 定义：```VALUE loopingRead()```
	- 功能描述：调用Read，如果读到末尾，调用resetIndex方法，从头开始再读，保证一定能够读到一个值
	- 参数：无
	- 返回值：下一个对象

3.  **get**

	- 定义：```VALUE get(int index)```
	- 功能描述：读取指定位置的对象，目前仅有MemoryDataBlock实现了该接口，该接口不会修改读索引
	-  参数
		- int index 对象索引
	- 返回值：下一个对象，若返回值为null，表明已经读到末尾

4. **put**

	- 定义：```void put(VALUE value)```
	- 功能描述：在写索引位置添加一个新元素，然后将写索引加1
	- 参数
		- VALUE value 待添加对象
	- 返回值：无

5. **resetReadIndex**

	- 定义：```void resetReadIndex()```
	- 功能描述：将读索引置为0，下次调用read会从头开始读取
	- 参数：无
	- 返回值：无

6. **clean**
	- 定义：```void clean()```
	- 功能描述：删除所有对象，并将读写索引置为0
	- 参数：无
	- 返回值：无

7. **shuffle**
	- 定义：```void shuffle()```
	- 功能描述：随机打乱对象的顺序，目前仅MemoryDataBlock支持该操作
	- 参数：无
	- 返回值：无

