# Project 1 MapReduce基础编程

## 需求1：对新闻标题进行分词，统计词频并按词频从高到低排序输出
### 1.实验设计说明：
#### 1.1设计思路：
  需求1需要完成的子任务包括：截取文本文档中的每一行数据中的标题部分、将标题进行分词、统计词语出现的次数以及按照词频从高到低进行排序。  
为此，需要建立两个串行的job，分别为job和job1。其中，job1需要等待job完成后才开始，job1的数据输入路径为job的数据输入路径。  
在job的mapper类中，需要完成获取行数据标题、将标题进行分词的任务并输出<word,one>对。在其mapper类中，需要完成词频统计任务并输出<word,frequency>对。  
在job1中，其mapper将读取job输出的<word,frequency>对，将word和frequency交换位置，形成<frequency,word>对，并通过自定义mapper的排列类，按词频从大到小进行排列并输出。job1中没有mapper类。
#### 1.2算法设计及程序说明：
  在job中，其mapper类将读取fulldata.txt中的每一行数据，通过分隔符\t将行数据进行分段，取其中的第五段至倒数第二段数据作为标题内容进行分词（适应fulldata.txt数据结构）。分词的具体做法是，先建立一个分词器，然后将标题段放入分词器中进行分词，得到一个List<Term>类型的数据，将该数据进行停词过滤。分词停词阶段完成后，对得到的List<Term>类型类型数据进行for循环，每一次循环取出一个词语赋给word，并输出<word,one>对。通过默认的combiner类，mapper类将得到的输入结果<word,[one,one,one,......,one]>对value值进行累加，最终得到<word,frequency>对并输出到指定目录上。  
  在job完成后，job1将job的输出作为输入，将行数据通过分隔符\t分段，第一段为word值，第二段为frequency值，然后将两个值进行存储。同时，建立setup（）类来得到从键盘中输入的词频阈值kvalue，将frequency与kvalue作比较，若frequency大于等于kvalue，则输出<frequency,word>对；否则不输出。最后，通过内置-super.compare（）函数对frequency进行比较，实现按照词频从大到小排序并输出。  
#### 1.3类说明：
  TokenizerMapper类：job中的mapper类，输入类型为<IntWritable,Text>,输出类型为<Text,IntWritable>。  
  IntSumReducer类：job中的combiner类（setCombinerClass（）的参数直接使用改reduce，用于防止map生成数据过大而无法传输的问题）。job中的reducer类，输入类型为<Text,[IntWritable,IntWritable,......]>,输出类型为<Text,IntWritable>。  
  SortMapper类：job1中的mapper类，输入类型为<IntWritable,Text>,输出类型为<IntWritable,Text>。  
  IntWritableDecreasingComparator类：job1的map中对key值进行排序的类。  
### 2.程序运行和实验结果
#### 2.1程序运行说明
  程序需要获取三个参数，分别为path1、path2和k,其中，path1为所用数据集fulldata.txt所在路径，path2为最终结果输出路径，k为词频阈值，若词频大于等于k，则输出，否则不输出。另外，中间数据的输出路径(即job的输出、job1的输入路径)在程序中已设定为固定路径，不再从键盘获取。  
  job的输出结果为所有<word,frequency>对，输出路径为程序固定路径；job1的输出结果为当frequency大于等于k时的<frequency，word>对，输出路径为path2。在job1中进行词频阈值处理。  
#### 2.2
