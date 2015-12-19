import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Test{

	public static class TestMap extends MapReduceBase implements Mapper<
Text,Text,Text,Text>{

	 Map<String,Map<String,Double>> train = new HashMap<String,Map<String,Double>>();

	//load the data of distributedfile into Map
	//train:HashMap<String,HashMap<String,Double>> key:nameOfClass,value:dictionary
	@Override
	public void configure(JobConf conf) {
		
		try{
		Path[] cacheFile = DistributedCache.getLocalCacheFiles(conf);
		Path trainParam = cacheFile[0]; 
		File f = new File(trainParam.toString());
		BufferedReader reader = new BufferedReader(new FileReader(f));
		String line = null;
		while((line = reader.readLine())!=null){
				String[] classAndDic = line.split("\t");
				Map<String,Double> dictionary = new HashMap<String,Double>();
				String[] dic = classAndDic[1].split(" "); 
				for(String s : dic){
					String[] tokenAndP = s.split(":");
					dictionary.put(tokenAndP[0],Double.parseDouble(tokenAndP[1]));
					}
				train.put(classAndDic[0],dictionary);
			}
		}catch(Exception e){
			e.printStackTrace();
		}

	}
	//inputTestData key:path of file  value:content of file
	public void map(Text key,Text value,OutputCollector<
	Text,Text>output,Reporter reporter) throws IOException{
		String[] tokens = value.toString().split(" ");
		String predictClass = predict(tokens,train);
		String[] path = key.toString().split("/");
		//(trueClass,predicClass)
		output.collect(new Text(path[path.length-1]),new Text(path[path.length-2]+" "+predictClass));
				
	}
}
	public static String predict(String[] tokens,Map<String,Map<String,Double>>train){
		String predictClass = null;
		double maxPredictValue = -Double.MAX_VALUE;
		for(String classname : train.keySet()){
			Map<String,Double> dictionary = train.get(classname);
			double predictValue = dictionary.get("probilityOfClass");
			for(String token : tokens){
				if(isPureNumber(token)){
					continue;
				}
				else if(dictionary.containsKey(token)){
					predictValue += dictionary.get(token);
				}
				else{
					predictValue += dictionary.get("default");
				}
			}
			if(predictValue > maxPredictValue){
				predictClass = classname;
				maxPredictValue = predictValue;
				}
			}
		return predictClass;	
	}

	public static Boolean isPureNumber(String token){
		Pattern p = Pattern.compile("[a-zA-z]"); 
		Matcher m = p.matcher(token);
		while(m.find()){
			return false;
		}
		return true;
	}

	public static class TestReduce extends MapReduceBase implements Reducer<
	Text,Text,Text,Text>{

		public void reduce(Text key,Iterator<Text> value,OutputCollector<Text,Text> output,Reporter reporter) throws IOException{
			output.collect(key,value.next());
			}
}

	public static void main(String[] args) throws Exception{
		String cachePath = "hdfs://HadoopMaster:9000/usr/my/data/naivebayes/output/train/part-00000";
		

		JobConf conf = new JobConf(Test.class);
		DistributedCache.addCacheFile(new URI(cachePath),conf);	

		conf.setJobName("Test");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(TestMap.class);
		conf.setReducerClass(TestReduce.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf,new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,new Path(args[1]));

		JobClient.runJob(conf);
		}
}
