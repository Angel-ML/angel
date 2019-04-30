# Storage Format
Angel's Models are stored in units of matrices. Each matrix corresponds to a folder named after it in the model storage path, which contains the matrix's metadata and data files. A matrix has only one corresponding metadata file but usually multiple data files, as most of Angel's models are derived from Parameter Server.

## Metadata File
Metadatas are stored in JSON format. A matrix metadata consists mainly of matrix features, partition indices and row-related indices, which are described by the **MatrixFilesMeta**, **MatrixPartitionMeta** and **RowPartitionMeta** classes respectively.
### MatrixFilesMeta
**Purpose**: matrix-related information

**Included Fields**:

 - **matrixId**: matrix's ID 
 - **rowType**: type of matrix (referring to RowType class)
 - **row**: number of rows
 - **blockRow**: number of rows of the partition block
 - **col**: number of columns 
 - **blockCol**: number of columns of the partition block
 - **matrixName**: name of the matrix
 - **formatClassName**: storage format of the matrix
 - **options**: other optional parameters
 - **partMetas**: index of the partition block

### MatrixPartitionMeta
**Purpose**: metadata of the partition block

**Included Fields**: 

- **startRow**: starting row of the partition block
- **endRow**: ending row of the partition block
- **startCol**: starting column of the partition block
- **endCol**: ending column of the partition block
- **nnz**: non-zero elements in the partition block (not being used temporarily)
- **fileName**: file name of the partition data
- **offset**: partition data's position in the file
- **length**: length of the partition data (bytes)
- **saveRowNum**: number of rows saved in the partition
- **saveColNum**: number of columns saved in the partition (useful only in column-major format)
- **saveColElemNum**: number of elements saved in one column (useful only in column-major format)
- **rowMetas**: row index of the partition

### RowPartitionMeta
**Purpose**: matadata of a specific row slice in a partition block
**Included Fileds**: 

- **rowId**: row index
- **offset**: row data's position in the file
- **elementNum**: number of elements stored in this row
- **saveType**: format of the file stored in this row

## Data File Format
Angel 2.0 adopts a user-defined model format. That is, the model output format can be customized according to practical needs. It's generally fine to use Angel's default output formats. Since Angel's default output formats are relatively simple, most of the models stored in defualt formats can be directly parsed without metadata files.


### Description of the Deaulf Formats
Angel provides 8 default model output formats:
`ValueBinaryRowFormat`, `ColIdValueBinaryRowFormat`, `RowIdColIdValueBinaryRowFormat`, `ValueTextRowFormat`, `ColIdValueTextRowFormat`, `RowIdColIdValueTextRowFormat`, `BinaryColumnFormat` and `TextColumnFormat`. These eight formats are described below.
#### ValueBinaryRowFormat
- **Description**: A binary format that contains only the values of the model. This format is only suitable for dense single-line models. Since there is no column number (feature index), the index range needs to be obtained from the model metadata.
- **Format**: |value|value|...|

#### ColIdValueBinaryRowFormat
- **Description**: A binary format that includes feature indices and corresponding values. This format can be applied to single-row models such as LR.
- **Format**: |index|value|index|value|...|

#### RowIdColIdValueBinaryRowFormat
- **Description**: A binary format that includes the row IDs, feature indices and corresponding values. This format can be applied to multiple-row models.
- **Format**: |rowid|index|value|rowid|index|value|...|

#### ValueTextRowFormat
- **Description**: A file format that only contains values of the model, with each value as a separate line. Similar to **ValueBinaryRowFormat**, this format is only suitable for dense single-line models. Also, since there is no column number (feature index), the index range needs to be obtained from the model metadata.
- **Format**: <br>
value<br>
value<br>
value

#### ColIdValueTextRowFormat
- **Description**: A file format that includes feature indices and corresponding values. Each row represents a feature ID-value pair separated by a comma by default. This format is suitable for single-line models such as LR.
- **Format**: <br>
index,value<br>
index,value<br>
index,value

#### RowIdColIdValueTextRowFormat
- **Description**: A file format that includes row IDs, feature indices and corresponding values. Each row represents a row ID-feature ID-value pair separated by a comma by default. This format is suitable for multiple-line models.
- **Format**: <br>
rowid,index,value<br>
rowid,index,value<br>
rowid,index,value

#### BinaryColumnFormat
- **Description**: A binary format that represents a matrix in column-major order. This format is currently used only in Embedding-related outputs (e.g. the Embedding layers in DNN, FM, etc.)
- **Format**: |index|row1 value|row2 value|...|index|row1 value|row2 value|...|

#### TextColumnFormat
- **Description**: A file format that represents a matrix in column-major order, with each row representing a column separated by commas by default. This format is currently used only in Embedding-related outputs (e.g. the Embedding layers in DNN, FM, etc.)
- **Format**: <br>
index,row1 value,row2 value,...<br>
index,row1 value,row2 value,...<br>
index,row1 value,row2 value,...

## Algorithm Output Format
Algorithms in Angel are currently implemented basing on a new computational graph framework, in which each layer can be individually formatted. **By default, SimpleInputLayer uses `ColIdValueTextRowFormat`, Embedding layer uses `TextColumnFormat`, FCLayer uses `RowIdColIdValueTextRowFormat`.**

- **LR, Linear Regression，SVM**: stored in `ColIdValueTextRowFormat` by default
- **GBDT**: `RowIdColIdValueTextRowFormat`
- **FM**: linear part uses `ColIdValueTextRowFormat`, while the Embedding layer uses `TextColumnFormat`
- **DeepFM, DNN, Wide And Deep, PNN, NFM, etc.**: linear part uses `ColIdValueTextRowFormat`, while the Embedding layer uses `TextColumnFormat`, and the fully connected part uses `RowIdColIdValueTextRowFormat`

Of course, if you don't want to use the default format, you can configure your model output format via the following parameters:

- ***ml.simpleinputlayer.matrix.output.format***: output format of SimpleInputLayer
- ***ml.embedding.matrix.output.format***: output format of Embedding
- ***ml.fclayer.matrix.output.format***: output format of FCLayer

Furthermore, if the 8 output formats provided by Angel cannot meet your requirements, you can also choose to extend the `RowFormat` or `ColumnFormat` class to customize your format at will. The detailed implementation is very simple, and the 8 formats that are currently available can provide useful references. After implementation, compile and pack up the new format, add it to Angel's depencencies using parameters provided by Angel, and configure via the four parameters mentioned above to use your custom output format.
