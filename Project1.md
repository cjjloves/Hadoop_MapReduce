# Project 1 MapReduce基础编程

## 需求1：对新闻标题进行分词，统计词频并按词频从高到低排序输出
### 1.实验设计说明：
#### 1.1设计思路：
需求1需要完成的子任务包括：截取文本文档中的每一行数据中的标题部分、将标题进行分词、统计词语出现的次数以及按照词频从高到低进行排序。
为此，需要建立两个串行的job，分别为job和job1。其中，job1需要等待job完成后才开始，job1的数据输入路径为job的数据输入路径。
```

	Job job = Job.getInstance();
	job.setJobName("wordcount");//建立job,命名为wordcount
  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//job数据输入路径由键盘输入
	FileOutputFormat.setOutputPath(job, new Path("/home/u2/hadoop_installs/hadoop-2.7.4/outputpro2"));//输出路径手动设置
```
