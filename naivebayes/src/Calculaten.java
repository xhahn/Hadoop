import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Calculaten{
	public static class Map extends MapReduceBase implements Mapper<
LongWritable,Text,Text,IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text Word = new Text();
	private final String isNumber = "[a-zA-Z]";
	
	public void map(LongWritable key,Text value,OutputCollector<
	Text,IntWritable>output,Reporter reporter) throws IOException{
		String word = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(word);
		String token = tokenizer.nextToken().split("\\.")[1];
		Pattern pattern = Pattern.compile(isNumber);
		Matcher m = pattern.matcher(token);
		while(m.find()){
			Word.set(word);
			output.collect(Word,one);
			break;
		}
		}
		}
	public static class Reduce extends MapReduceBase implements Reducer<
	Text,IntWritable,Text,IntWritable>{
	public void reduce(Text key,Iterator<IntWritable> values,
	OutputCollector<Text,IntWritable> output,Reporter reporter) throws IOException{
		int sum = 0;
		while(values.hasNext()){
			sum += values.next().get();
		}
		output.collect(key,new IntWritable(sum));
}}

	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(Calculaten.class);
		conf.setJobName("Calculaten");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf,new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,new Path(args[1]));

		JobClient.runJob(conf);
		}
}
