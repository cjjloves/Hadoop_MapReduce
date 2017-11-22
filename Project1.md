# Project 1 MapReduce基础编程

## 需求1：对新闻标题进行分词，统计词频并按词频从高到低排序输出
### 1 实验设计说明：
#### 1.1 设计思路：
&emsp;&emsp;需求1需要完成的子任务包括：截取文本文档中的每一行数据中的标题部分、将标题进行分词、统计词语出现的次数以及按照词频从高到低进行排序。  
&emsp;&emsp;为此，需要建立两个串行的job，分别为job和job1。其中，job1需要等待job完成后才能开始，job1的数据输入路径为job的数据输出路径。  
&emsp;&emsp;在job的mapper类中，需要完成获取行数据标题、将标题进行分词的任务并输出<word,one>对。在其mapper类中，需要完成词频统计任务并输出<word,frequency>对。  
&emsp;&emsp;在job1中，其mapper将读取job输出的<word,frequency>对，将word和frequency交换位置，形成<frequency,word>对，并通过自定义mapper的排列类，按词频从大到小进行排列并输出。job1中没有reducer类。
#### 1.2 算法设计及程序说明：
&emsp;&emsp;在job中，其mapper类将读取fulldata.txt中的每一行数据，通过分隔符\t将行数据进行分段，取其中的第五段至倒数第二段数据作为标题内容进行分词（适应fulldata.txt数据结构）。分词的具体做法是，先建立一个分词器，然后将标题段放入分词器中进行分词，得到一个List<Term>类型的数据，将该数据进行停词过滤。分词停词阶段完成后，对得到的List<Term>类型类型数据进行for循环，每一次循环取出一个词语赋给word，并输出<word,one>对。通过默认的combiner类，reducer类将得到的输入结果<word,[one,one,one,......,one]>对value值进行累加，最终得到<word,frequency>对并输出到指定目录上。  
&emsp;&emsp;在job完成后，job1将job的输出作为输入，将行数据通过分隔符\t分段，第一段为word值，第二段为frequency值，然后将两个值进行存储。同时，建立setup（）类来得到从键盘中输入的词频阈值kvalue，将frequency与kvalue作比较，若frequency大于等于kvalue，则输出<frequency,word>对；否则不输出。最后，通过内置-super.compare（）函数对frequency进行比较，实现按照词频从大到小排序并输出。  
#### 1.3类说明：
&emsp;&emsp;TokenizerMapper类：job中的mapper类，输入类型为<IntWritable,Text>,输出类型为<Text,IntWritable>。  
&emsp;&emsp;IntSumReducer类：job中的combiner类（setCombinerClass（）的参数直接使用该reducer类，用于防止map生成数据过大而无法传输的问题）。job中的reducer类，输入类型为<Text,[IntWritable,IntWritable,......]>,输出类型为<Text,IntWritable>。  
&emsp;&emsp;SortMapper类：job1中的mapper类，输入类型为<Text,IntWritable>,输出类型为<IntWritable,Text>。  
&emsp;&emsp;IntWritableDecreasingComparator类：job1的map中对key值进行从大到小排序的类。  
### 2.程序运行和实验结果
#### 2.1程序运行说明
&emsp;&emsp;程序需要获取三个参数，分别为path1、path2和k,其中，path1为待处理数据集fulldata.txt所在路径，path2为最终结果输出路径，k为词频阈值，若词频大于等于k，则输出，否则不输出。另外，中间数据的输出路径(即job的输出、job1的输入路径)在程序中已设定为固定路径，不再从键盘获取。  
&emsp;&emsp;job的输出结果为所有<word,frequency>对，输出路径为程序固定路径；job1的输出结果为当frequency大于等于k时的<frequency，word>对，输出路径为path2。在job1中进行词频阈值处理。  
&emsp;&emsp;源程序如下：
	
```
package project;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
public class Project {
	public static class TokenizerMapper //定义Map类实现字符串分解
	extends Mapper<Object, Text, Text, IntWritable>
	{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	public void map(Object key, Text value, Context context)
	//public void map()
	throws IOException, InterruptedException
	{ //将字符串拆解成单词
	//String[] gettitle = value.toString().split("  ");
		String[] gettitle = value.toString().split("\t");
		for(int i=4;i<gettitle.length-1;i++){
			String sentence =gettitle[i];
	Segment segment = HanLP.newSegment().enablePartOfSpeechTagging(false); 
	List<Term> segWords = segment.seg(sentence); 
	CoreStopWordDictionary.apply(segWords); 
	
	for (int i1 = 0; i1 < segWords.size(); i1++) {  
		word.set(segWords.get(i1).toString());
		context.write(word,one);
        //System.out.println(segWords.get(i));  
        }  
	}
	
	}
	}
	
	//定义Reduce类规约同一key的value
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
	private IntWritable result = new IntWritable();
	//实现reduce()函数
	public void reduce(Text key, Iterable<IntWritable> values, Context context )
	throws IOException, InterruptedException
	{
	int sum = 0;
	//遍历迭代器values，得到同一key的所有value
	for (IntWritable val : values) { sum += val.get(); }
	result.set(sum);
	context.write(key, result);
	}
	}
	
	public static class SortMapper //定义Map类实现字符串分解
	extends Mapper<Object, Text, IntWritable,Text>
	{
			private int num;
			private IntWritable frequency= new IntWritable();
			private Text words=new Text();
			protected void setup(Context context) throws IOException,InterruptedException{
				num = context.getConfiguration().getInt("kvalue", 0);
			}
			public void map(Object key,Text value, Context context)
					throws IOException, InterruptedException{
				String[] line=value.toString().split("\t");
				int fre = Integer.parseInt(line[1]);
				frequency.set(fre);
				words.set(line[0]);
				//System.out.println(num);
				if(fre>=num)
				context.write (frequency,words);
			}
	} 

	 private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

	     public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	                return -super.compare(b1, s1, l1, b2, s2, l2);
	       }
	}
	
	
	public static void main(String[] args) throws Exception
	{
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length != 3)
	{ System.err.println("Usage: wordcount <in> <out> <k value>");
	System.exit(3);
	}
	Job job = Job.getInstance();
	job.setJobName("wordcount");
	job.setJarByClass(Project.class);//设置执行任务的jar
	job.setMapperClass(TokenizerMapper.class); 
	job.setCombinerClass(IntSumReducer.class); //设置Combine类
	job.setReducerClass(IntSumReducer.class); //设置Reducer类
	job.setOutputKeyClass(Text.class); //设置job输出的key
	//设置job输出的value
	job.setOutputValueClass(IntWritable.class);
	//设置输入文件的路径
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path("/home/u2/hadoop_installs/hadoop-2.7.4/outputpro2"));
	
	Job job1 = Job.getInstance();
	job1.setJobName("sort");
	job1.getConfiguration().setInt("kvalue", Integer.parseInt(args[2]));
	job1.setJarByClass(Project.class);//设置执行任务的jar
	job1.setMapperClass(SortMapper.class);
	//job.setCombinerClass(IntSumReducer.class); //设置Combine类
	//job.setReducerClass(IntSumReducer.class); //设置Reducer类
	job1.setOutputKeyClass(IntWritable.class); //设置job输出的key
	//设置job输出的value
	job1.setOutputValueClass(Text.class);
	//设置输入文件的路径
	job1.setSortComparatorClass(IntWritableDecreasingComparator.class);//从大到小排序
	FileInputFormat.addInputPath(job1, new Path("/home/u2/hadoop_installs/hadoop-2.7.4/outputpro2"));
	//设置输出文件的路径
	FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
	//提交任务并等待任务完成*/
	if (job.waitForCompletion(true)) {
	System.exit(job1.waitForCompletion(true) ? 0 : 1); }
	//System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
	
}
```
#### 2.2 实验结果说明
&emsp;&emsp;由于job没有对阈值进行处理，所以输出结果是所有的<word,frequency>对。  
job结果的前五行数据如下  
```
28274	公告
21477	股份
18811	有限公司
12138	公司
9107	上市公司
```
最后五行数据如下
```
1	天职
1	天翼
1	贾文军
1	天网
1	悍马
```
&emsp;&emsp;在job1中对阈值进行了处理，使得只有当frequency大于等于kvalue值时才会输出<frequency,word>对。  
job1结果的前五行数据如下（与job相同）  
```
28275	公告
21575	股份
18811	有限公司
12179	公司
9163	上市公司
```
最后五行数据如下（假定kvalue值为100）
```
100	中药
100	迎风
100	单车
100	力度
100	年薪
```
### 3 程序的不足与可能的改进之处
#### 3.1 程序的不足
&emsp;&emsp;程序性能方面的不足在于程序无法只根据特定的分词表进行分词。该程序基于hanpl插件(补充中详细说明)进行分词，所用的词典为hanlp插件提供的词典，自定义词典只能作为一种补充，而无法作为分词所依赖的唯一词典。另外，程序性能方面的不足还表现在对download_data文件夹中的文档进行处理时所需要的时间非常长（这个程序处理download_data文件夹所需要的时间大概为6分钟），而处理fulldata.txt则非常快速（大概需要20秒左右）。不过，这可能是hadoop自身的问题，而与程序无关。  
&emsp;&emsp;程序扩展性的不足在于对初始数据的处理中获取标题段的阶段，由于不同的数据的数据结构可能不同，所以采用\t分隔符对行数据进行分段，并且取第五段至倒数第二段为标题段的方法可能无法适用于所有需要处理的数据。
#### 3.2 程序可能改进指处
&emsp;&emsp;对于程序无法只根据特定分词表进行分词的问题，可能的解决方法是对特定分词表进行修改，使得该特定分词表的数据结构与hanlp插件提供的词典的数据结构相同，然后用该特的分词表替代hanlp插件提供的词典。特定分词表中的数据只有词语，而hanlp插件提供的词典中数据格式为词语、词性和频次。比较合理的解决方法是模仿hanlp插件的源代码，自己实现一个分词插件程序。  
&emsp;&emsp;对于程序扩展性不足的问题，比较合理的解决方法是规定处理数据的数据结构，免得多次修改程序。

## 需求2：对词语进行带URL属性的文档倒排索引
### 1 程序设计说明：
#### 1.1 设计思路：
&emsp;&emsp;需求2需要完成的子任务包括：截取文本文档每一行数据中的标题部分与URL部分、将标题进行分词、对同一词语相关联的URL值进行合并。  
&emsp;&emsp;为此，只需要建立一个ob，在该job中，其mapper将读取行数据，通过分隔符\t取得标题段并将标题分词（该过程与需求1中的相同），取最后一段数据作为URL段，并输出<word,URL>对。该job的reducer通过combiner得到输入<word,[URL1,URL2,...]>,将所有URL值合并到Text中，最后输出<word,Text>对。
#### 1.2 算法设计及程序说明：
&emsp;&emsp;在job中，其mapper类先对数据进行分词（分词原理与需求1相同，不再赘述），然后取最后一段为URL值（适应fulldata.txt数据结构），分别对word和URL进行赋值，输出<word,URL>对。reducer类首先设定一个阈值count,对通过默认combiner类得到的输入<word,[URL1,URL2,...]>进行有限次循环，每次循环取一个URL值并合并到Text中，每两个URL之间用一个分号进行分隔。最后输出<word,Text>对。  
#### 1.3 类说明：
&emsp;&emsp;TokenizerMapper类：job中的mapper类，输入类型为<IntWritable,Text>,输出类型为<Text,Text>。  
&emsp;&emsp;IntSumReducer类：job中的combiner类。job中的reducer类，输入类型为<Text,[Text,Text,......]>,输出类型为<Text,Text>。  
### 2 程序运行和实验结果
#### 2.1 程序运行说明
&emsp;&emsp;程序需要获取两个参数，分别为path1、path2,其中，path1为待处理数据集fulldata.txt所在路径，path2为最终结果输出路径。  
&emsp;&emsp;job的输出结果为所有<word,Text>对，输出路径为path2。其中，Text为与词语相关联的前count个URL值的合并值。  
源程序如下：
```
package project3;

import java.io.IOException;
import java.util.List;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import project3.Project3;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;

public class Project3 {
	public static class TokenizerMapper //定义Map类实现字符串分解
	extends Mapper<Object, Text, Text, Text>
	{
	private Text word = new Text();
	private Text url1 = new Text();
	//实现map()函数
	public void map(Object key, Text value, Context context)
	//public void map()
	throws IOException, InterruptedException
	{ //将字符串拆解成单词
	String[] gettitle = value.toString().split("\t");
	String url=gettitle[gettitle.length-1];
	url1.set(url.toString());
	for(int i=4;i<gettitle.length-1;i++)
	{
		String sentence =gettitle[i];
		Segment segment = HanLP.newSegment().enablePartOfSpeechTagging(false); 
		List<Term> segWords = segment.seg(sentence); 
		CoreStopWordDictionary.apply(segWords);  
		for (int i1 = 0; i1 < segWords.size(); i1++) {  
			word.set(segWords.get(i1).toString());
			context.write(word,url1); 
	  }  
	}
	}
	}
	
	//定义Reduce类规约同一key的value
	public static class IntSumReducer extends Reducer<Text,Text,Text,Text>
	{
	private Text result = new Text();
	//实现reduce()函数
	public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
	throws IOException, InterruptedException
	{
		//int count=0;
		String fileList = new String();
		for (Text value : values) {
			fileList += value.toString() + ";";
			count++;
			//if(count>=9)
				//break;
		}
		fileList = fileList.substring(0,fileList.length()-1);
		result.set(fileList);
		context.write(key, result);	
	}
	}
	
	public static void main(String[] args) throws Exception
	{ 
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length != 2)
	{ System.err.println("Usage: wordcount <in> <out>");
	System.exit(2);
	}
	Job job = Job.getInstance();
	job.setJobName("url");
	job.setJarByClass(Project3.class);//设置执行任务的jar
	job.setMapperClass(TokenizerMapper.class); 
	job.setCombinerClass(IntSumReducer.class); //设置Combine类
	job.setReducerClass(IntSumReducer.class); //设置Reducer类
	job.setOutputKeyClass(Text.class); //设置job输出的key
	//设置job输出的value
	job.setOutputValueClass(Text.class);
	//设置输入文件的路径
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
}
```
#### 2.2 实验结果说明
&emsp;&emsp;由于设置了阈值count，所以输出<word,URLs>对中最多包含count个URL。  
此处假定只进行了10次循环（即最多合并前10个URL值），则输出结果前五行数据如下（数据说明：以下输出中的符号在hanlp插件给定词典中均有包括，但在特定停词表没有包括，所以输出的词语中存在特殊符号）
```
%-	http://finance.sina.com.cn/stock/t/2017-01-06/doc-ifxzkfvn0640228.shtml;http://finance.sina.com.cn/stock/s/2017-03-13/doc-ifychavf2639718.shtml;http://finance.sina.com.cn/roll/2017-01-10/doc-ifxzkfvn1215778.shtml;http://finance.sina.com.cn/stock/t/2017-07-26/doc-ifyihrwk2551543.shtml;http://finance.sina.com.cn/stock/t/2017-05-24/doc-ifyfkqwe0968988.shtml;http://finance.sina.com.cn/stock/t/2017-06-26/doc-ifyhmpew3498837.shtml;http://finance.sina.com.cn/stock/t/2017-04-05/doc-ifyeayzu6826764.shtml;http://finance.sina.com.cn/stock/t/2017-01-15/doc-ifxzqnva3639896.shtml;http://finance.sina.com.cn/stock/t/2017-07-06/doc-ifyhwefp0187054.shtml
%--	http://finance.sina.com.cn/stock/t/2017-06-23/doc-ifyhmtrw3753415.shtml;http://finance.sina.com.cn/stock/t/2017-07-06/doc-ifyhweua4140597.shtml;http://finance.sina.com.cn/roll/2016-09-26/doc-ifxwermp3935274.shtml
%H	http://finance.sina.com.cn/stock/hkstock/marketalerts/2017-08-03/doc-ifyitamv4688698.shtml
%ST	http://finance.sina.com.cn/roll/2017-08-01/doc-ifyinryq7371215.shtml;http://finance.sina.com.cn/roll/2017-08-01/doc-ifyinryq7371215.shtml;http://finance.sina.com.cn/roll/2017-08-01/doc-ifyinryq7371215.shtml
%~	http://finance.sina.com.cn/stock/t/2017-01-25/doc-ifxzutkf2624237.shtml
```
输出结果的最后五行数据如下：
```
龙韵	http://finance.sina.com.cn/roll/2016-04-13/doc-ifxrcuyk2863171.shtml;http://finance.sina.com.cn/stock/t/2016-03-31/doc-ifxqxcnr5090532.shtml;http://finance.sina.com.cn/stock/t/2016-04-13/doc-ifxrcizu4148645.shtml;http://finance.sina.com.cn/roll/2016-04-13/doc-ifxrcizu4152778.shtml;http://finance.sina.com.cn/roll/2016-04-26/doc-ifxrprek3460148.shtml;http://finance.sina.com.cn/roll/2016-05-10/doc-ifxryahs0652498.shtml;http://finance.sina.com.cn/roll/2016-05-25/doc-ifxsqtya6051109.shtml;http://finance.sina.com.cn/stock/t/2016-06-06/doc-ifxsuypf5077425.shtml;http://finance.sina.com.cn/stock/t/2016-06-06/doc-ifxsuypf5082817.shtml
龙马	http://finance.sina.com.cn/stock/s/2017-07-03/doc-ifyhryex5872774.shtml;http://finance.sina.com.cn/stock/s/2017-07-03/doc-ifyhryex5872774.shtml;http://finance.sina.com.cn/roll/2016-09-29/doc-ifxwmamz0019802.shtml;http://finance.sina.com.cn/stock/s/2017-07-03/doc-ifyhryex5872774.shtml;http://finance.sina.com.cn/stock/t/2017-01-19/doc-ifxzunxf1419082.shtml;http://finance.sina.com.cn/stock/t/2016-09-27/doc-ifxwevww1699844.shtml;http://finance.sina.com.cn/stock/t/2017-03-24/doc-ifycsukm3511890.shtml;http://cj.sina.com.cn/article/detail/5160876646/200146;http://finance.sina.com.cn/stock/t/2017-04-06/doc-ifyecfnu7420109.shtml
龚昕	http://finance.sina.com.cn/stock/t/2017-06-29/doc-ifyhrxtp6303641.shtml
龚曙光	http://finance.sina.com.cn/roll/2016-08-26/doc-ifxvitex8981146.shtml
龚虹嘉	http://finance.sina.com.cn/roll/2017-05-03/doc-ifyetxec7367838.shtml;http://finance.sina.com.cn/roll/2017-05-03/doc-ifyetxec7367838.shtml;http://finance.sina.com.cn/roll/2017-05-03/doc-ifyetxec7367838.shtml;http://finance.sina.com.cn/roll/2017-05-03/doc-ifyetxec7367838.shtml;http://finance.sina.com.cn/roll/2017-05-03/doc-ifyetxec7367838.shtml;http://finance.sina.com.cn/roll/2017-05-03/doc-ifyetxec7367838.shtml;http://finance.sina.com.cn/roll/2017-05-03/doc-ifyetxec7367838.shtml;http://finance.sina.com.cn/roll/2017-05-03/doc-ifyetxec7367838.shtml;http://finance.sina.com.cn/roll/2017-05-03/doc-ifyetxec7367838.shtml
```
### 3 程序的不足与可能的改进之处
#### 3.1 程序的不足
&emsp;&emsp;程序性能方面的不足在于其reduce阶段，由于reduce阶段的工作是将多个URL值进行合并，所以当一个词语出现了几千次的时候需要进行几千次循环来进行URL值合并，这种合并在该程序中是串行实现的，占据了程序的大量处理时间。  
&emsp;&emsp;程序扩展性的不足在于不能适应不同结构的数据需求（与需求1相同，不再赘述）。另外，其不足还在于程序会根据hanlp中给定的词典将特定字符视为词语（如实验结果中的前五行数据），而这些字符很可能是我们所不希望看到的。
#### 3.2 程序可能改进指处
&emsp;&emsp;对于程序reduce阶段合并URL值只能串行进行的问题，合理的解决方法是自定义partitioner，如果一个词语相关联的URL值过多，则将该URL串进行划分，然后分别传到不同的节点上进行处理。然后再增加一个job，其reducer类用于合并相同词语的URL串。  
&emsp;&emsp;对于程序扩展性不足的问题，最简单的方法是在停词表中写入不希望看到的特定字符或词语，但这种方法较为繁琐。
## 补充
### 1 配置说明
#### 1.1 eclipse安装hadoop插件说明
&emsp;&emsp;需要从网上下载hadoop-eclipse-plugin-1.1.2.jar，并将其拷贝到 eclipse的plugins目录，配置classpath。然后创建Map/Reduce Project,配置端口信息，其中端口号需要与core-site.xml中的端口号保持一致。程序编写完毕后，需要对Run Configurations的Arguments进行输入参数配置，即可运行。安装hadoop插件的好处是，无需在创建项目后在添加关于hadoop的jar包，能够直接在eclipse运行hadoop程序并得到结果。  
&emsp;&emsp;参考网站：https://www.cnblogs.com/shishanyuan/p/4178732.html
#### 1.2 hanlp分词插件说明
&emsp;&emsp;使用hanlp插件，需要添加关于hanlp的jar包（本程序添加的是hanlp-1.2.8.jar），还需要引入data文件夹和hanlp.properties配置文件，其中，能通过在data/dictionary/custom添加分词表来实现添加自定义分词表的功能，同时能够在data/dictionary/stopwords.txt中增加停词。hanlp.properties配置文件需要放入所创建的project下的bin文件夹中，其内容中的root路径指向data所在文件夹，能够在CustomDictionaryPath路径中添加自定义分词文档。  
&emsp;&emsp;hanlp分词插件配置参考网站：http://blog.csdn.net/a_step_further/article/details/50333961  
&emsp;&emsp;本程序使用的是该网站中的第二种方法。  
&emsp;&emsp;该分词插件无法使用自定义词典作为唯一分词词典，原因是其核心词典的分词结构为【词语】【词性A】【A的频次】【词性B】【B的频次】，而自定义词典中只包含词语，所以如果用自定义词典代替hanlp插件的核心词典的话，会因为没有词性和频次等属性而出错。但能够直接用特定的停词表作为唯一停词表，原因是停词表的结构只包含词语。  
&emsp;&emsp;hanlp分词插件详情参考网站：http://hanlp.linrunsoft.com/doc/_build/html/dictionary.html
### 2 程序问题发现  
#### 2.1 需求2的URL合并问题
&emsp;&emsp;在需求2中，需要将combiner的参数设置为reducer类，否则reducer无法得到输入数据<word,[URL1,URL2,......]>。  
&emsp;&emsp;Combiner参数设置理论参考网站：https://zhidao.baidu.com/question/1959117107842078420.html 
#### 2.2 数据集问题发现
&emsp;&emsp;数据集中“承德露露”一类数据有问题，具体问题是有一行数据中多了一个“\n”符号。将其删除后整个数据集（fulldata.txt或download_data）能够正常运行。
