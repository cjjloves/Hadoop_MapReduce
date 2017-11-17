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
  源程序如下：    
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
	
	
	public class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
	    protected void reduce(IntWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
	        for(Text value : values){
	            context.write(value, key);
	        }
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

#### 2.2
