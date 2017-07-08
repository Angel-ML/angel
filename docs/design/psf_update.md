# **update型psFunc**

--

## 接口的定义

```Java
Future<VoidResult> update(UpdaterFunc update) throws AngelException;
```
其中参数类型是一个UpdaterFunc对象，该对象封装了update psf方法的参数和执行流程：

```Java
public abstract class UpdaterFunc {
  private final UpdaterParam param;
  public UpdaterFunc(UpdaterParam param) {
    this.param = param;
  }
  public UpdaterParam getParam() {return param;}
  public abstract void partitionUpdate(PartitionUpdaterParam partParam);
}
``` 
update psf参数类型为UpdaterParam。UpdateParam对象与GetParam对象类似，它除了包含update的具体参数外，也有一个split方法，该方法的作用是将全局的update参数按照矩阵分区进行划分，得到的结果是一个分区update参数列表，即PartitionUpdateParam对象列表。

与get psf不同，update pof的执行流程只有一步：即以矩阵分区为单位分别进行update操作，这个过程由partitionUpdate方法表示。update psf没有具体的返回值，只返回给应用程序一个Future，应用程序可以选择是否等待操作完成。

上述提到的UpdaterParam, PartitionUpdaterParam类以及partitionUpdater方法都可以由用户自由扩展。这样可以定制自己所需的任何参数更新方式。

## **update psf实现流程**
update psf执行流程需要PS Client和PS共同完成。上述提到的UpdaterParam划分和最后的merge方法是在PS Client执行的；而partitionUpdate方法是在PS端执行的。具体的流程如下图所示，左子图表示PS Client处理流程，右子图表示PS端处理流程：

![][1]

## **update psf编程示例**
下面是一个简单的使用update psf实现将矩阵某一行设置为指定范围随机数的例子。

```Java
public class Random extends UpdaterFunc {
 // Random函数的参数
 public static class RandomUpdaterParam extends UpdaterParam {
   // 行号
   private final int rowIndex;
   // 随机数下界
   private final double min;
   // 随机数上界
   private final double max;

   public RandomUpdaterParam(int matrixId, int rowIndex, double min, double max) {
     super(matrixId);
     this.rowIndex = rowIndex;
     this.min = min;
     this.max = max;
    }

   // 生成分区random参数列表
   @Override
   public List<PartitionUpdaterParam> split() {
     // 获取矩阵包含的分区列表
     List<PartitionKey> parts =
         PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);
     int size = parts.size();

     List<PartitionUpdaterParam> partParams = new ArrayList<PartitionUpdaterParam>(size);

     // 如果一个分区包含指定行号，则为他生成一个分区random参数
     for (int i = 0; i < size; i++) {
       if(rowIndex >= parts.get(i).getStartRow() && rowIndex < parts.get(i).getEndRow()) {
         partParams.add(new PartitionRandomParam(matrixId, parts.get(i), rowIndex, min, max));
        }   
      }

     return partParams;
    }
   
   public int getRowIndex() {
     return rowIndex;
    }
  }
 
 // 分区Random参数
 public static class PartitionRandomParam extends PartitionUpdaterParam {
   // 行号
   private int rowIndex;
   // 随机数下界
   private double min;
   // 随机数上界
   private double max;
   
   public PartitionRandomParam(int matrixId, PartitionKey partKey, int rowIndex, double min, double max) {
     super(matrixId, partKey);
     this.rowIndex = rowIndex;
     this.min = min;
     this.max = max;
    }
   
   // 定义一个无参构造函数，序列化/反序列化用
   public PartitionRandomParam() {
     this(-1, null, -1, -1.0, -1.0);
    }
 
   // 将分区random参数序列化到一个buffer中
   @Override
   public void serialize(ByteBuf buf) {
     super.serialize(buf);
     buf.writeInt(rowIndex);
     buf.writeDouble(min);
     buf.writeDouble(max);
    }

   // 从buffer中反序列化出分区random参数
   @Override
   public void deserialize(ByteBuf buf) {
     super.deserialize(buf);
     rowIndex = buf.readInt();
     min = buf.readDouble();
     max = buf.readDouble();
    }

   // 估算所需buffer大小
   @Override
   public int bufferLen() {
     return 20 + super.bufferLen();
    }

   public int getRowIndex() {
     return rowIndex;
    }

   public double getMin() {
     return min;
    }

   public double getMax() {
     return max;
    }
  }
 
 public Random(UpdaterParam param) {
   super(param);
  }
 
 public Random() {
   this(null);
  }

 // 分区random操作
 @Override
 public void partitionUpdate(PartitionUpdaterParam partParam) {    
   PartitionRandomParam randomParam = (PartitionRandomParam) partParam;

   // 获取矩阵分区
   ServerPartition part =
       PSContext.get().getMatrixPartitionManager()
           .getPartition(randomParam.getMatrixId(), randomParam.getPartKey().getPartitionId());

   if (part != null) {
     // 获取参数
     int rowIndex = randomParam.getRowIndex();
     double min = randomParam.getMin();
     double max = randomParam.getMax();
     java.util.Random r = new java.util.Random();
     // 获取指定行分片
     ServerDenseDoubleRow row = (ServerDenseDoubleRow) part.getRow(rowIndex);
     if (row != null) {
       int size = row.size();
       try {
         row.getLock().writeLock().lock();
         // 将该行分片的每一个成员设置为指定范围的随机数
         for(int i= 0; i < size; i++) {
           row.set(i, r.nextDouble() * (max - min) + min);
          }
       } finally {
         row.getLock().writeLock().unlock();
        }
      }
    }
  }
}
```


将代码编译后打成jar包，在提交任务时通过参数angel.lib.jars上传该jar包，然后就可以在应用程序中调用了。调用方式如下：

```Java
Random randomFunc = new Random(new RandomParam(matrixId, rowIndex, 0.0, 1.0));
psModel.update(randomFunc).get();
```


  [1]: ../img/psf_update.png