import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NBTrain{
	public static class Map extends MapReduceBase implements Mapper<
LongWritable,Text,Text,Text>{

	 HashMap<String,Double> pc = new HashMap<String,Double>();
	 HashMap<String,Integer> NB = new HashMap<String,Integer>();

	//load the data of distributedfile into Map
	//pc:Map<String,double>,key:nameOfClass,value:pc
	//NB:Map<String,int>,key:nameOfClass,value:N+B
	@Override
	public void configure(JobConf conf) {
		
		try{
		Path[] cacheFile = DistributedCache.getLocalCacheFiles(conf);
		Path pcParam = cacheFile[0]; 
		Path NBParam = cacheFile[1];
		System.out.println("===========1111============= ");

		File f = new File(pcParam.toString());
		File f2 = new File(NBParam.toString());
		BufferedReader reader = new BufferedReader(new FileReader(f));
		BufferedReader reader2 = new BufferedReader(new FileReader(f2));
		String line = null;
		Pattern p = Pattern.compile("(\\S+)\\s+(\\d\\.\\d+)");
		while((line = reader.readLine())!=null){
				Matcher m = p.matcher(line);
				if(m.find()){
					System.out.println(line);
					pc.put(m.group(1),Double.parseDouble(m.group(2)));
					}
			}
		System.out.println(pc.size()+"1111 ");
		line = null;
		int B = 0;
		List<Integer> tmpB = new ArrayList();
		Pattern p2 = Pattern.compile("(\\S+)\\s+(\\d+)\\s+(\\d+)");
		while((line = reader2.readLine())!=null){
				Matcher m2 = p2.matcher(line);
				if(m2.find()){
					int tmpN = Integer.parseInt(m2.group(2));
					tmpB.add(Integer.parseInt(m2.group(3)));
					NB.put(m2.group(1),tmpN);
					}
		}
		for(Iterator i = tmpB.iterator();i.hasNext();)
			B += (int)i.next();

		Set<String> s = NB.keySet();
		for(String str : s)
			NB.put(str,NB.get(str)+B);

		}catch(Exception e){
		
			}

		System.out.println(pc.size()+"2222 "+NB.size());
	}
	//inputData key:nameOfClass.token value:n
	public void map(LongWritable key,Text value,OutputCollector<
	Text,Text>output,Reporter reporter) throws IOException{
		String line = value.toString();
		String classtoken = null;
		String n = null;
		double probility = 0.0;
		System.out.println(pc.size()+"33333 "+NB.size());
		System.out.println("============3333============ ");
		//an example of a record:ALS.hello  34
		Pattern p = Pattern.compile("(\\S+)\\s+(\\d+)");
		Matcher m = p.matcher(line);
		if(m.find()){
			classtoken = m.group(1);
			n = m.group(2);
			}
		//split the class and token
		//ct[0]:class name
		//ct[1]:token name
		String[] ct = classtoken.split("\\.",2);
		
		probility = Math.log(1.0*(Integer.parseInt(n)+1)/NB.get(ct[0]));
		//the form of value is "token:probility"
		String tp = ct[1]+":"+probility;
		output.collect(new Text(ct[0]),new Text(tp));
				}
	}

	public static class Reduce extends MapReduceBase implements Reducer<
	Text,Text,Text,Text>{
	HashMap<String,Double> pc = new HashMap<String,Double>();
	HashMap<String,Integer> NB = new HashMap<String,Integer>();

	//load the data of distributedfile into Map
	//pc:Map<String,double>,key:nameOfClass,value:pc
	//NB:Map<String,int>,key:nameOfClass,value:N+B
	@Override
	public void configure(JobConf conf) {
		
		try{
		Path[] cacheFile = DistributedCache.getLocalCacheFiles(conf);
		Path pcParam = cacheFile[0]; 
		Path NBParam = cacheFile[1];
		System.out.println("===========1111============= ");

		BufferedReader reader = new BufferedReader(new FileReader(new File(pcParam.toString())));
		BufferedReader reader2 = new BufferedReader(new FileReader(new File(NBParam.toString())));
		String line = null;
		Pattern p = Pattern.compile("(\\S+)\\s+(\\d\\.\\d+)");
		while((line = reader.readLine())!=null){
				Matcher m = p.matcher(line);
				if(m.find()){
					System.out.println(line);
					pc.put(m.group(1),Double.parseDouble(m.group(2)));
					}
			}
		System.out.println(pc.size()+"1111 ");
		line = null;
		int B = 0;
		List<Integer> tmpB = new ArrayList();
		Pattern p2 = Pattern.compile("(\\S+)\\s+(\\d+)\\s+(\\d+)");
		while((line = reader2.readLine())!=null){
				Matcher m2 = p2.matcher(line);
				if(m2.find()){
					int tmpN = Integer.parseInt(m2.group(2));
					tmpB.add(Integer.parseInt(m2.group(3)));
					NB.put(m2.group(1),tmpN);
					}
		}
		for(Iterator i = tmpB.iterator();i.hasNext();)
			B += (int)i.next();

		Set<String> s = NB.keySet();
		for(String str : s)
			NB.put(str,NB.get(str)+B);
		}catch(Exception e){
			}

		System.out.println(pc.size()+"2222 "+NB.size());
	}
	public void reduce(Text key,Iterator<Text> values,
	OutputCollector<Text,Text> output,Reporter reporter) throws IOException{
		String cp = values.next().toString();
		while(values.hasNext()){
			cp += " "+values.next().toString();
			}
		//when one token is not in this class
		String default_p = "default:"+Math.log(1.0/NB.get(key.toString()));
		cp += " "+default_p;
		cp += " "+"probilityOfClass:"+Math.log(pc.get(key.toString()));
		output.collect(key,new Text(cp));
		}
}

	public static void main(String[] args) throws Exception{
		String[] cachePath = {
			"hdfs://HadoopMaster:9000/usr/my/data/naivebayes/input/train/pc.txt",
			"hdfs://HadoopMaster:9000/usr/my/data/naivebayes/input/train/NB.txt"
			};

		JobConf conf = new JobConf(NBTrain.class);
		DistributedCache.addCacheFile(new URI(cachePath[0]),conf);	
		DistributedCache.addCacheFile(new URI(cachePath[1]),conf);	

		conf.setJobName("NBTrain");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf,new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,new Path(args[1]));

		JobClient.runJob(conf);
		}
}
