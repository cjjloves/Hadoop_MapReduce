# K-Means对散点进行聚类及可视化
## 1 程序设计说明
### 1.1 键盘输出变量说明
&emsp;&emsp;k:簇个数  
&emsp;&emsp;iterationNum：迭代次数  
&emsp;&emsp;sourcePath：散点数据所在路径  
&emsp;&emsp;outputPath：簇属性及散点所属簇数据所在路径  
&emsp;&emsp;row：散点个数
### 1.2 程序类说明
&emsp;&emsp;TDKmeansDriver：主函数所在类，用于调用其他类  
&emsp;&emsp;cresample：随机生成散点数据类。该类从TDKmeansDriver中获取散点生成路径和散点个数变量，利用Random().nextInt()函数产生随机散点  
&emsp;&emsp;Generator：随机生成簇类。该类从TDKmeansDriver中获得散点生成路径、簇生成路径、簇个数和散点个数变量，利用Random().nextInt()函数为每一个散点赋值，取有最大的前簇个数值的散点作为初始簇  
&emsp;&emsp;clusters：簇迭代类。该类从TDKmeansDriver中获得簇生成路径、散点数据所在路径、簇个数和散点个数变量，map阶段计算散点到每个簇的距离，返回距离最近的簇的ID；reduce阶段加权平均计算新簇的属性  
&emsp;&emsp;kmeanscluster：生成散点所属簇类。该类与clusers相似，但输出的是每一个散点的属性即其所属的簇
### 1.3 数据可视化说明
&emsp;&emsp;将最终的的数据导入RStudio中利用R语言实现数据可视化
## 2 代码
### 2.1 TDKmeansDriver
```
public class TDKmeansDriver {

	private int k;
	private int iterationNum;
	private String sourcePath;
	private String outputPath;
	private Configuration conf;//声明变量
	
	public TDKmeansDriver(int k, int iterationNum, String sourcePath, String outputPath, Configuration conf){
		this.k = k;
		this.iterationNum = iterationNum;
		this.sourcePath = sourcePath;
		this.outputPath = outputPath;
		this.conf = conf;
	}
	public void clusterCenterJob() throws IOException, InterruptedException, ClassNotFoundException{
		for(int i = 0;i < iterationNum; i++){
			Job clusterCenterJob = Job.getInstance();
			clusterCenterJob .setJobName("clusterCenterJob" + i);
			clusterCenterJob .setJarByClass(clusters.class);
			clusterCenterJob.getConfiguration().setInt("kvalue", k);
			clusterCenterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + i);
			clusterCenterJob.setMapperClass(clusters.KMeansClusterMapper.class);
			clusterCenterJob.setMapOutputKeyClass(DoubleWritable.class);
			clusterCenterJob.setMapOutputValueClass(Text.class);
			//clusterCenterJob.setCombinerClass(clusters.IntSumReducer.class);
			clusterCenterJob.setReducerClass(clusters.IntSumReducer.class);
			clusterCenterJob.setOutputKeyClass(NullWritable.class);
			clusterCenterJob.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(clusterCenterJob, new Path(sourcePath));
			FileOutputFormat.setOutputPath(clusterCenterJob, new Path(outputPath + "/cluster-" + (i + 1) +"/"));
			clusterCenterJob.waitForCompletion(true);
			System.out.println("finished!");
		}
	}
	public void KMeansClusterJod() throws IOException, InterruptedException, ClassNotFoundException{
		Job clusterCenterJob = Job.getInstance();
		kMeansClusterJob.setJobName("KMeansClusterJob");
		kMeansClusterJob.setJarByClass(kmeanscluster.class);
		
		kMeansClusterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + (iterationNum - 1) +"/");

		kMeansClusterJob.setMapperClass(kmeanscluster.KMeansClusterMapper.class);
		kMeansClusterJob.setMapOutputKeyClass(NullWritable.class);
		kMeansClusterJob.setMapOutputValueClass(Text.class);

		kMeansClusterJob.setNumReduceTasks(0);

		FileInputFormat.addInputPath(kMeansClusterJob, new Path(sourcePath));
		FileOutputFormat.setOutputPath(kMeansClusterJob, new Path(outputPath + "/clusteredInstances" + "/"));
		
		kMeansClusterJob.waitForCompletion(true);
		System.out.println("finished!");
	}
	public void generateInitialCluster(int row) throws NumberFormatException, IOException{
		File folder = new File(outputPath+"/cluster-0");
		folder.mkdirs();
		Generator generator = new Generator(sourcePath,outputPath,k,row);
	}
	private void Cresample(String sourcePath2, int row) throws IOException {
		File folder = new File(sourcePath2);
		folder.mkdirs();
		cresample createtxtCresample=new cresample(sourcePath2,row);
	}
	private void clustergetk(int k2) {
		clusters k=new clusters(k2);
		
	}
	private void kmeansclustergetk(int k2) {
		kmeanscluster k=new kmeanscluster(k2);
		
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		System.out.println("start");
		Configuration conf = new Configuration();
		int k = Integer.parseInt(args[0]);
		int iterationNum = Integer.parseInt(args[1]);
		String sourcePath = args[2];
		String outputPath = args[3];
		int row = Integer.parseInt(args[4]);
		TDKmeansDriver driver = new TDKmeansDriver(k, iterationNum, sourcePath, outputPath, conf);
		driver.Cresample(sourcePath,row);
		driver.generateInitialCluster(row);
		System.out.println("initial cluster finished");
		driver.clustergetk(k);
		driver.clusterCenterJob();
		driver.kmeansclustergetk(k);
		driver.KMeansClusterJod();
	}
	
	
}
```
### 2.2 cresample
```
public class cresample {
	public cresample(String path, int row) throws IOException{
	       int[][] array=new int[row][2];
	       for(int i=0,j=array.length;i<j;i++){
	           for(int h=0,k=array[i].length;h<k;h++){
	              array[i][h]=new Random().nextInt(50); 
	           }
	       }
	       File file = new File(path+"/Instance");
	       try{
	       file.createNewFile();}
	       catch(Exception e){
	    	   System.out.println("cannot create new file!");
	       }
	       PrintWriter pw = new PrintWriter(new FileWriter(file));
	       //遍历
	       for(int i=0,j=array.length;i<j;i++){
	          // System.out.println();
	           for(int h=0,k=array[i].length;h<k;h++){
	        	   if(h==0)
	        		   pw.print(array[i][h]+",");
	        	   else
	        		   pw.print(array[i][h]+"\n");
	           }
	       }
	      pw.close();
	}
}

```
### 2.3 Generator
```
public class Generator {
	public Generator(String filePath1,String filePath2,int k,int row) throws NumberFormatException, IOException{
		int[][] array1=new int[row][2];
		FileInputStream fis=new FileInputStream(filePath1+"/Instance");
		InputStreamReader isr=new InputStreamReader(fis, "UTF-8");
		BufferedReader br = new BufferedReader(isr);
		String line="";
	    String[] arrs=null;
	    int count=0;
	    while ((line=br.readLine())!=null) {
	    		arrs=line.split(",");
	            //System.out.println(arrs[0]);
	    		array1[count][0]=Integer.parseInt(arrs[0]);
	    		array1[count][1]=Integer.parseInt(arrs[1]);
	    		count++;
	    }
	    br.close();
	    isr.close();
	    fis.close();
	    int[] array2=new int[row];
	    for(int count1=0;count1<row;count1++){
	    	array2[count1]=new Random().nextInt(1024);
	    	//System.out.println(array2[count1]);
	    }
	    int idnum=0;
	    int big=0;
	    int[][] array3=new int[k][3];
	    for(int kvalue=0;kvalue<k;kvalue++){
	    	for(int count1=0;count1<row;count1++){
	    		if(array2[count1]>=big){
	    			idnum=count1;
	    			big=array2[count1];}
	    	}
	    array3[kvalue][0]=kvalue+1;
	    array3[kvalue][1]=array1[idnum][0];
	    array3[kvalue][2]=array1[idnum][1];
	    //System.out.println(array3[kvalue][0]);
	    array2[idnum]=0;
	    //System.out.println(array2[idnum]);
	    //System.out.println(idnum);
	    idnum=0;
	    big=0;
	    }
		
	    
	    	
	       File file = new File(filePath2+"/cluster-0/part-r-00000");
	       try{
	       file.createNewFile();}
	       catch(Exception e){
	    	   System.out.println("cannot create new file!");
	       }
	       PrintWriter pw = new PrintWriter(new FileWriter(file));
	       //遍历
	       for(int i=0,j=array3.length;i<j;i++){
	          // System.out.println();
	           for(int h=0,another=array3[i].length;h<another;h++){
	        	   if(h==0)
	        		   pw.print((double)array3[i][h]+",");
	        	   else if(h==1)
	        		   pw.print((double)array3[i][h]+",");
	        	   else
	        		   pw.print((double)array3[i][h]+"\n");
	           }
	       }
	      pw.close();
	    
	    
	    
	}
}
	
```
### 2.4 clusters
```
public class clusters {
	public static int k;
	public clusters(int row){
		k=row;
	}
	public static class KMeansClusterMapper extends Mapper<LongWritable, Text,DoubleWritable,Text>{
		
		
		
		private double[][] array3=new double[k][3];	
		private DoubleWritable id = new DoubleWritable();
		protected void setup(Context context) throws IOException,InterruptedException{
			//FileSystem fs = FileSystem.get(context.getConfiguration());
			//FileInputStream fis=new FileInputStream("/home/u2/hadoop_installs/hadoop-2.7.4/kmeans1/Cluster");
			//k = context.getConfiguration().getInt("kvalue", 0);
			FileInputStream fis=new FileInputStream(context.getConfiguration().get("clusterPath")+"/part-r-00000");
			//System.out.println(context.getConfiguration().get("clusterPath"));
			InputStreamReader isr=new InputStreamReader(fis, "UTF-8");
			BufferedReader br = new BufferedReader(isr);
			String line="";
		    String[] arrs=null;
		    int count=0;
		    while ((line=br.readLine())!=null) {
		    		arrs=line.split(",");
		            //System.out.println(arrs[0]);
		    		array3[count][0]=Double.parseDouble(arrs[0]);
		    		//System.out.println(array3[count][0]);
		    		array3[count][1]=Double.parseDouble(arrs[1]);
		    		//System.out.println(array3[count][1]);
		    		array3[count][2]=Double.parseDouble(arrs[2]);
		    		
		    		count++;
		    }
		    br.close();
		    isr.close();
		    fis.close();
			
		}
		
		public void map(LongWritable key, Text value, Context context)throws 
		IOException, InterruptedException{
			
			String[] getxy = value.toString().split(",");
			int xvalue=Integer.parseInt(getxy[0]);
			//System.out.println(xvalue);
			int yvalue=Integer.parseInt(getxy[1]);
			int big=20000;
			double idnum=0;
			int distance=0;
			for(int count=0;count<k;count++){
				distance=((int)array3[count][1]-xvalue)*((int)array3[count][1]-xvalue)+((int)array3[count][2]-yvalue)*((int)array3[count][2]-yvalue);
				if(distance<=big){
					big=distance;
					idnum=array3[count][0];
					//System.out.println(array3[count][0]);
				}
			}
			//System.out.println(idnum);
			id.set(idnum);
			context.write(id, value);
			
			
		}
		
	}
	
	
	public static class IntSumReducer extends Reducer<DoubleWritable,Text,NullWritable,Text>
	{
		private Text result = new Text();
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context )
				throws IOException, InterruptedException
				{
					double count=0;
					//System.out.println(String.valueOf(key));
					double xsum=0;
					double ysum=0;
					for (Text value : values) {
						String[] stringxy = value.toString().split(",");
						//System.out.println(stringxy[0]);
						xsum+=Double.parseDouble(stringxy[0]);
						ysum+=Double.parseDouble(stringxy[1]);
						count++;
						//System.out.println(xsum);
					}
					//System.out.println(xsum);
					double xaverage=xsum/count;
					double yaverage=ysum/count;
					//System.out.println(xaverage);
					result.set(String.valueOf(key)+","+String.valueOf(xaverage)+","+String.valueOf(yaverage));
					context.write (NullWritable.get(),result);
					//System.out.println(result);
				}
		
	}
	
	
	
}
```
### 2.5 kmeanscluster
```
public class kmeanscluster {
	
	public static int k;
	public kmeanscluster(int row){
		k=row;
	}
	
	
	public static class KMeansClusterMapper extends Mapper<LongWritable, Text,NullWritable,Text>{

		private double[][] array3=new double[k][3];	
		//private DoubleWritable id = new DoubleWritable();
		protected void setup(Context context) throws IOException,InterruptedException{
			//FileSystem fs = FileSystem.get(context.getConfiguration());
			//FileInputStream fis=new FileInputStream("/home/u2/hadoop_installs/hadoop-2.7.4/kmeans1/Cluster");
			FileInputStream fis=new FileInputStream(context.getConfiguration().get("clusterPath")+"/part-r-00000");
			//System.out.println(context.getConfiguration().get("clusterPath"));
			InputStreamReader isr=new InputStreamReader(fis, "UTF-8");
			BufferedReader br = new BufferedReader(isr);
			String line="";
		    String[] arrs=null;
		    int count=0;
		    while ((line=br.readLine())!=null) {
		    		arrs=line.split(",");
		            //System.out.println(arrs[0]);
		    		array3[count][0]=Double.parseDouble(arrs[0]);
		    		//System.out.println(array3[count][0]);
		    		array3[count][1]=Double.parseDouble(arrs[1]);
		    		//System.out.println(array3[count][1]);
		    		array3[count][2]=Double.parseDouble(arrs[2]);
		    		
		    		count++;
		    }
		    br.close();
		    isr.close();
		    fis.close();
			
		}
		private Text result = new Text();
		public void map(LongWritable key, Text value, Context context)throws 
		IOException, InterruptedException{
			
			String[] getxy = value.toString().split(",");
			int xvalue=Integer.parseInt(getxy[0]);
			//System.out.println(xvalue);
			int yvalue=Integer.parseInt(getxy[1]);
			int big=20000;
			double idnum=0;
			int distance=0;
			for(int count=0;count<k;count++){
				distance=((int)array3[count][1]-xvalue)*((int)array3[count][1]-xvalue)+((int)array3[count][2]-yvalue)*((int)array3[count][2]-yvalue);
				if(distance<=big){
					big=distance;
					idnum=array3[count][0];
					//System.out.println(array3[count][0]);
				}
			}
			//System.out.println(idnum);
			result.set(getxy[0].toString()+"\t"+getxy[1].toString()+"\t"+String.valueOf((int)idnum));
			//id.set(idnum);
			context.write(NullWritable.get(),result);
			
			
		}
		
	}
}
```
### 2.6 RStudio实现数据可视化
```
print(getwd())//得到RStudio运行目录
data <- read.table("part-m-00000")
x=data$V1
y=data$V2
color=data$V3
plot(x,y,col=color)
```
## 3 结果展示
### 假定散点个数为100，簇个数为5
### 3.1 随机散点数据（前5行）
```
15,35
22,48
25,47
12,49
44,40
```
### 3.2 初始簇生成数据
```
1.0,22.0,33.0
2.0,15.0,4.0
3.0,23.0,17.0
4.0,42.0,41.0
5.0,31.0,31.0
```
###  3.3 最后一次迭代生成的簇数据
```
1.0,13.26923076923077,41.11538461538461
2.0,9.08695652173913,16.0
3.0,32.5,12.3
4.0,41.63636363636363,38.27272727272727
5.0,28.05,29.75
```
### 3.4 带有散点所属簇的数据（取前10行）
```
15	35	1
22	48	1
25	47	1
12	49	1
44	40	4
14	47	1
5	30	1
27	11	3
27	34	5
1	30	1
```
### 3.5 散点可视化
![Image text](https://raw.github.com/cjjloves/hello-world/master/picture/visualization.png)
# 4 补充
## 4.1 本代码与参考代码的异同
&emsp;&emsp;本代码增加了随机散点生成类  
&emsp;&emsp;在选取初始簇方面，本代码对每一个散点赋予一个随机数，取随机数较大的前k个作为初始簇，复杂度为O（簇个数乘以散点个数）；参考代码先取前k个散点作为默认簇，在对后面的第i个散点取一个从0到i-1的随机数，若该随机数为0，则保留该散点并对该散点再取一个从0到簇个数-1的随机数以确定代替的原始簇，复杂度为O（2乘以散点个数）  
&emsp;&emsp;在读取簇信息方面，本代码用一个列为2的二维数组保存簇信息；参考代码则是用一个ArrayList<Cluster>类来保存。参考代码这种方法适用于多维数据，而本代码只适用于二维数据  
&emsp;&emsp;mapreduce阶段处理过程基本相同，只是最后的输出结果形式不同，本代码的输出结果为“散点x值\t散点y值\t散点所属类id”,输出结果服务于数据可视化
## 4.2 不足
&emsp;&emsp;本代码因为随机生成散点以及随机选取初始簇，可视化效果并不是很好
