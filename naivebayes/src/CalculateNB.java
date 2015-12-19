import java.io.IOException;
import java.util.*;
import java.io.DataOutput;
import java.io.DataInput;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class CalculateNB{

	public static class IntPair implements WritableComparable<IntPair>{
		private IntWritable first;
		private IntWritable second;

		public IntPair(){
			set(new IntWritable(),new IntWritable());
			}
		public IntPair(int first,int second){
			set(new IntWritable(first),new IntWritable(second));
			}
		public IntPair(IntWritable first,IntWritable second){
			set(first,second);
			}
		public void set(IntWritable first,IntWritable second){
			this.first = first;
			this.second = second;
			}
		public IntWritable getFirst(){return first;}
		public IntWritable getSecond(){return second;}

		@Override
		public void write(DataOutput out)throws IOException{
			first.write(out);
			second.write(out);
			}
		@Override
		public void readFields(DataInput in)throws IOException{
			first.readFields(in);
			second.readFields(in);
			}
		@Override
		public String toString(){
			return first+" "+second;
			}
		@Override
		public int compareTo(IntPair ip){
			int cmp = first.compareTo(ip.first);
			if(cmp!=0)
				return cmp;
			return second.compareTo(ip.second);
			}
		}

public static class Map extends MapReduceBase implements Mapper<
LongWritable,Text,Text,IntPair>{
	private final static IntWritable one = new IntWritable(1);
	private Text Word = new Text();
	private IntPair intpair = new IntPair();

	public void map(LongWritable key,Text value,OutputCollector<
	Text,IntPair>output,Reporter reporter) throws IOException{
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		Word.set(tokenizer.nextToken().split("\\.")[0]);
		intpair.set(new IntWritable(Integer.parseInt(tokenizer.nextToken())),one);
		output.collect(Word,intpair);
		}
		}
	public static class Reduce extends MapReduceBase implements Reducer<
	Text,IntPair,Text,IntPair>{
	public void reduce(Text key,Iterator<IntPair> values,
	OutputCollector<Text,IntPair> output,Reporter reporter) throws IOException{
		int N = 0;
		int B = 0;
		while(values.hasNext()){
			IntPair ip = values.next();
			N += ip.getFirst().get();
			B += ip.getSecond().get();
		}
		output.collect(key,new IntPair(N,B));
}}

	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(CalculateNB.class);
		conf.setJobName("CalculateNB");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntPair.class);

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
