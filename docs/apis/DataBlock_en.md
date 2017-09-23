# DataBlock\<VALUE\>

---

> DataBlock is a base class of data block management and storage; it provides a basic data access interface, suitable for the *write once, read multiple times* scenario (for example, storing and accessing training data set)

## Functionality

DataBlock can be viewed as an "**array**" that can grow dynamically, where newly added objects can only be appended to the end of the array; it maintains read/write index information internally. Angel implemented 3 basic interfaces for different data storage media:

 - **MemoryDataBlock**: store data in memory; support random access and sequential access
 - **DiskDataBlock**: store data in local disk; currently only support sequential access
 - **MemoryAndDiskDataBlock**: try storing data in memory first, but write data to local disk when running out of memory

## Core Interfaces

1. **registerType**
	- Definition: ```void registerType(Class<VALUE> valueClass)```
	- Functionality: sets the object type for storage
	- Parameters:
		- Class<VALUE> valueClass: type of the object to be stored
	- Return value: none

2. **read**
	- Definition: ```VALUE read()```
	- Functionality: adds 1 to read index; reads the object at the index position; returns null when it reaches the end of DataBlock
	- Parameters: none
	- Return value: next object or null

8. **loopingRead**
	- Definition: ```VALUE loopingRead()```
	- Functionality: calls `read`; if it reaches the end, calls `resetIndex` and restarts from the beginning position to ensure reading a value
	- Parameters: none
	- Return value: next object

3.  **get**

	- Definition: ```VALUE get(int index)```
	- Functionality: reads the object at the specified position; currently only implemented for `MemoryDataBlock`; doesn't modify the read index
	- Parameters: 
		- int index: object index
	- Return value: next object or null if it reaches the end of DataBlock

4. **put**

	- Definition: ```void put(VALUE value)```
	- Functionality: adds an object at the write index and increase write index by 1
	- Parameters:
		- VALUE value: object to be added
	- Return value: none

5. **resetReadIndex**

	- Definition: ```void resetReadIndex()```
	- Functionality: sets read index to 0, next call of `read` will start from the beginning position of DataBlock
	- Parameters: none
	- Return value: none

6. **clean**
	- Definition: ```void clean()```
	- Functionality: deletes all objects and sets read/write indices to 0
	- Parameters: none
	- Return value: none

7. **shuffle**
	- Definition: ```void shuffle()```
	- Functionality: shuffles the order of objects in DataBlock; currently only supported in the `MemoryDataBlock` interface
	- Parameters: none
	- Return value: none

