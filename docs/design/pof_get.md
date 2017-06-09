## get
### **get psf接口定义**
get方法的定义如下：

```Java
GetResult get(GetFunc get) throws AngelException;
```
其中参数类型是一个GetFunc对象，该对象封装了get psf方法的参数和执行流程：
```Java
public abstract class GetFunc {
  protected final GetParam param;
  public GetFunc(GetParam param) {
    this.param = param;
  }
  public GetParam getParam() {return param;}
  public abstract PartitionGetResult partitionGet(PartitionGetParam partParam);
  public abstract GetResult merge(List<PartitionGetResult> partResults);
}

``` 
get psf参数类型为GetParam，GetParam实现了ParamSplit接口，ParamSplit接口定义了一个split方法，该方法的含义是将一个针对整个矩阵的全局的参数划分成一个矩阵分区参数列表。GetParam类型提供了一个默认的split接口实现，也就是针对该矩阵的每一个分区都生成一个矩阵分区的参数。 get psf的矩阵分区参数是一个PartitionGetParam类型。

get psf的执行流程分为两步，分别由接口partitionGet和merge方法表示。partitionGet方法定义了从一个矩阵分区获取所需结果的具体流程，它的返回结果类型为PartitionGetResult。merge方法定义了如何将各个矩阵分区的结果合并得到完整结果的过程。完整的结果类型为GetResult。

上述提到的GetParam, PartitonGetParam, PartitionGetResult, GetResult类以及partitionGet和merge方法都可以由用户自由扩展。这样可以定制自己所需的任何参数获取方式方法。

### **get psf实现流程**
get psf执行流程需要PS Client和PS共同完成。上述提到的get参数划分和最后的merge方法是在PS Client执行的；而partitionGet方法是在PS端执行的。具体的流程如下图所示，左子图表示PS Client端处理流程，右子图表示PS端处理流程：

![][1]

### **get psf编程示例**
获取矩阵某一行的所有值的和
```Java
public class Sum extends GetFunc {
  //Sum函数的参数，包含行号
  public static class SumParam extends GetParam {
    private final int rowIndex;
    public SumParam(int matrixId, int rowIndex) {
      super(matrixId);
      this.rowIndex = rowIndex;
    }
    
    public int getRowIndex() {
      return rowIndex;
    }
    
    // 为每一个包含指定行的矩阵分区生成一个PartitionSumParam对象
    @Override
    public List<PartitionGetParam> split() {
      // 获取矩阵的分区列表
      List<PartitionKey> parts =
          PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);
      int size = parts.size();

      List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(size);
      
      // 遍历分区列表，判断分区是否包含指定行，如果包含，则生成一个PartitionSumParam对象
      for (int i = 0; i < size; i++) {
        if(rowIndex >= parts.get(i).getStartRow() && rowIndex < parts.get(i).getEndRow()) {
          partParams.add(new PartitionSumParam(matrixId, parts.get(i), rowIndex));
        }   
      }

      return partParams;
    }    
  }
  
  // 针对某一个分区的求和操作所需要的参数
  public static class PartitionSumParam extends PartitionGetParam {
    private int rowIndex;
    
    public PartitionSumParam(int matrixId, PartitionKey partKey, int rowIndex) {
      super(matrixId, partKey);
      this.rowIndex = rowIndex;
    }
    
    // 需要定义一个无参数的构造函数，序列化/反序列化要用
    public PartitionSumParam() {
      this(-1, null, -1);
    }
    
    // 将参数序列化到buffer中
    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowIndex);
    }

    // 从buffer中反序列化出参数
    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowIndex = buf.readInt();
    }
    
    // 用于估算buffer大小
    @Override
    public int bufferLen() {
      return 4 + super.bufferLen();
    }

    public int getRowIndex() {
      return rowIndex;
    }
  }
  
  // 定义函数最终返回结果
  public static class SumResult extends GetResult {
    private final double result;
    public SumResult(double result) {
      this.result = result;
    }
    public double getResult() {
      return result;
    }
  }
  
  // 每一个分区sum的结果
  public static class PartitionSumResult extends PartitionGetResult {
    private double result;
    
    public PartitionSumResult(double result) {
      super();
      this.result = result;
    }
    
    // 需要一个无参数的构造函数，序列化和反序列化用
    public PartitionSumResult() {
      this(-1.0);
    }
    
    // 序列化结果到buffer中
    @Override
    public void serialize(ByteBuf buf) {
      buf.writeDouble(result);
    }

    // 从buffer中反序列化得到结果
    @Override
    public void deserialize(ByteBuf buf) {
      result = buf.readDouble();
    }
 
    // 估算buffer长度
    @Override
    public int bufferLen() {
      return 8;
    }

    public double getResult() {
      return result;
    }
  }

  public Sum(SumParam param) {
    super(param);
  }

  // 获取落在该分区的行分片的元素的和，本方法会在PS端调用
  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartitionSumParam sumParam = (PartitionSumParam) partParam;
    
    // 获取矩阵分区
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(sumParam.getMatrixId(), sumParam.getPartKey().getPartitionId());

    double result = 0.0;
    if (part != null) {
      // 获取行分片
      int rowIndex = sumParam.getRowIndex();;
      ServerDenseDoubleRow row = (ServerDenseDoubleRow) part.getRow(rowIndex);
      if (row != null) {
        // 计算行分片元素的和
        DoubleBuffer data = row.getData();
        int size = row.size();
        for (int i = 0; i < size; i++) {
          result += data.get(i);
        }
      }
    }

    // 将结果封装成PartitionSumResult对象
    return new PartitionSumResult(result);
  }

  // 将各个partition的执行结果合并，得到最终结果，本方法在PS Client端调用
  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    double finalResult = 0.0;
    int size = partResults.size();
   
    // 遍历所有分区返回的结果，对它们进行求和，得到最终结果
    for(int i = 0; i < size; i++) {
      finalResult += ((PartitionSumResult)partResults.get(i)).getResult();
    }
    
    // 将结果封装成SumResult对象
    return new SumResult(finalResult);
  }

}
```

将代码编译后打成jar包，在提交任务时通过参数angel.lib.jars上传该jar包，然后就可以在应用程序中调用了。调用方式如下：
```Java
Sum sumFunc = new Sum(new SumParam(matrixId, rowIndex));
double result = ((SumResult)psModel.get(sumFunc)).getResult();
```


  [1]: ../img/get%20psf%E6%B5%81%E7%A8%8B.png