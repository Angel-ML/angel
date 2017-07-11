# 模型分区
为了支持大模型，Angel需要将模型分成多个部分分别存于不同的PS节点上。模型的划分方式是一个值得关注的问题，不同的划分方式可能导致计算性能上的差异，一个好的划分方式应该是保证不同的PS节点负载尽量相等。在实际应用中，各个算法和一个算法的各种实现可能对划分方式要求都不一样。正因如此，Angel提供了默认划分算法用来满足一般的划分需求，同时也提供了自定义划分功能满足特殊需求。

# Angel默认的模型分区方式
在Angel中，模型使用矩阵来表示。当应用程序没有指定矩阵划分参数或者方法时，Angel将使用默认的划分算法，默认的划分算法遵循以下几个原则：

 - 尽量将一个模型平均分配到所有PS节点上
 - 对于非常小的模型，将它们尽量放在一个PS节点上
 - 对于多行的模型，尽量将同一行放在一个PS节点上

Angel底层的部分RPC接口是以矩阵分区为单位来操作的，为了避免消息过大，Angel有一个最大消息限制100MB（可通过参数`angel.netty.matrixtransfer.max.message.size` 配置）。所以一般矩阵分区占用存储空间不要超过这个限制值，一般推荐的单个矩阵分区大小为10MB~40MB。以分区大小40MB为例，相当于40 * 10^6 / 8 = 5000000个double型元素。

默认分区算法如下：
假定row为整个矩阵的行数，col为整个矩阵的列数，serverNum为PS个数。

 - 当矩阵行数大于PS个数时：

	分区块行数 `blockRow = Math.min(row / serverNum, Math.max(1, 5000000 / col))`
	分区块列数 `blockCol = Math.min(5000000 / blockRow, col)`

 - 当矩阵行数小于PS个数时：

	分区块行数 `blockRow = row`
	分区块列数 `blockCol = Math.min(5000000 / blockRow, Math.max(100, col / serverNum))`
	
	举几个例子，假设serverNum = 4，下图是几个典型的矩阵分区大小：
	![][1]
	
# 自定义模型分区
首先，Angel允许用户自定义矩阵分区的大小，方法是在定义矩阵时使用带有矩阵分区参数的构造函数：`MatrixContext(String name, int rowNum, int colNum, int maxRowNumInBlock, int maxColNumInBlock)` 具体可参考[MatrixContext]()。

这种方式可以控制矩阵分区的大小，但仍然不够灵活。例如当我们的矩阵各个部分访问频率并不一样时，可能需要不同的分区有不同的大小，或者说我们需要将某些存在关联的分区放到同一个PS上。为了满足一些特殊算法和模型的需求，我们抽象了一个分区接口Partitioner，用户可以通过实现Partitioner接口来实现自定义的模型分区方式。

Partitioner接口的定义如下：
``` java
interface Partitioner {
  /**
   * Init matrix partitioner
   * @param mContext matrix context
   * @param conf
   */
  void init(MatrixContext mContext, Configuration conf);

  /**
   * Generate the partitions for the matrix
   * @return the partitions for the matrix
   */
  List<MLProtos.Partition> getPartitions();

  /**
   * Assign a matrix partition to a parameter server
   * @param partId matrix partition id
   * @return parameter server index
   */
  int assignPartToServer(int partId);
}

```

主要需要实现的接口有两个`getPartitions`和`assignPartToServer`。`getPartitions`表示获取该矩阵的分区列表；`assignPartToServer`表示将一个分区分配给某一个PS。

下面通过一个简单的例子来说明如何通过实现Partitioner接口来自定义矩阵划分方式。假设有这样的使用场景：模型是一个`3 * 10000000`维的矩阵，PS个数为8，第一行访问的更为频繁，我希望分区的小一些，划分成4个分区，其他行的划分成2个分区，如下图所示：
![][2]

由于分区的大小并不相同，所以默认的分区方式没有办法实现这样的需求，这个时候可以定制一个分区类MyPartitioner：
```java
public class MyPartitioner implements Partitioner {
  /* Matrix context: matrix dimension, matrix element type etc*/
  private MatrixContext mContext;

  /* Application configuration */
  private Configuration conf;

  /* PS number */
  private int psNum;

  @Override public void init(MatrixContext mContext, Configuration conf) {
    this.mContext = mContext;
    this.conf = conf;
    psNum =
      conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, AngelConfiguration.DEFAULT_ANGEL_PS_NUMBER);
  }

  @Override public List<MLProtos.Partition> getPartitions() {
    List<MLProtos.Partition> partitions = new ArrayList<MLProtos.Partition>(6);
    int row = mContext.getRowNum();
    int col = mContext.getColNum();

    int blockCol = col / 4;
    int partitionId = 0;

    // Split the first row to 4 partitions
    for (int i = 0; i < 4; i++) {
      if (i < 3) {
        partitions.add(MLProtos.Partition.newBuilder().setMatrixId(mContext.getId())
          .setPartitionId(partitionId++).setStartRow(0).setEndRow(1).setStartCol(i * blockCol)
          .setEndCol((i + 1) * blockCol).build());
      } else {
        partitions.add(MLProtos.Partition.newBuilder().setMatrixId(mContext.getId())
          .setPartitionId(partitionId++).setStartRow(0).setEndRow(1).setStartCol(i * blockCol)
          .setEndCol(col).build());
      }
    }

    blockCol = col / 2;
    // Split other row to 2 partitions
    for (int rowIndex = 1; rowIndex < row; rowIndex++) {
      partitions.add(
        MLProtos.Partition.newBuilder().setMatrixId(mContext.getId()).setPartitionId(partitionId++)
          .setStartRow(rowIndex).setEndRow(rowIndex + 1).setStartCol(0).setEndCol(blockCol)
          .build());
      partitions.add(
        MLProtos.Partition.newBuilder().setMatrixId(mContext.getId()).setPartitionId(partitionId++)
          .setStartRow(rowIndex).setEndRow(rowIndex + 1).setStartCol(blockCol).setEndCol(col)
          .build());
    }

    return partitions;
  }

  @Override public int assignPartToServer(int partId) {
    // Assign a partition to a PS, return the index of PS. PS index is in range [0 ~ psNum)
    return partId % psNum;
  }
}
```
将代码编译后打成jar包，在提交任务时通过参数angel.lib.jars上传该jar包，然后就可以在应用程序中调用了。调用方式如下：

```java
MatrixContext mContext = new MatrixContext("my_matrix", 3, 10000000);
mContext.setPartitioner(new MyPartitioner());
```

[1]: ../img/matrix_partition.png
[2]: ../img/partitioner_example.png
