package naivebayes;
import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;

	public class WholeFileInputFormat extends FileInputFormat<NullWritable,BytesWritable>{
		
		public WholeFileInputFormat(){};

		@Override
		protected boolean isSplitable(JobContext context,Path file){
			return false;
			}

		@Override
		public RecordReader<NullWritable,BytesWritable>createRecordReader(InputSplit split,TaskAttemptContext context)throws IOException,InterruptedException{
			WholeFileRecordReader reader = new WholeFileRecordReader();
			reader.initialize(split,context);
			return reader;
			}

		public static class WholeFileRecordReader extends RecordReader<NullWritable,BytesWritable>{
		private FileSplit fileSplit;
		private Configuration conf;
		private BytesWritable value = new BytesWritable();
		private boolean processed = false;
		
		@Override
		public void initialize(InputSplit split,TaskAttemptContext context)throws IOException{
			this.fileSplit = (FileSplit)split;
			this.conf = context.getConfiguration();
			}

		@Override
		public NullWritable getCurrentKey()throws IOException,InterruptedException{
			return NullWritable.get();
			}

		@Override
		public BytesWritable getCurrentValue()throws IOException,InterruptedException{
			return value;
			}

		@Override
		public boolean nextKeyValue()throws IOException,InterruptedException{
			if(!processed){
				byte[] contents = new byte[(int)fileSplit.getLength()];
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(conf);
				FSDataInputStream in = null;
				try{
					in = fs.open(file);
					IOUtils.readFully(in,contents,0,contents.length);
					value.set(contents,0,contents.length);
					}
				finally{
					IOUtils.closeStream(in);
					}
				processed = true;
				return true;
				}
			return false;
			}

		@Override
		public float getProgress()throws IOException{
			return processed?1.0f:0.0f;
			}

		@Override
		public void close()throws IOException{
			}
		}
}
	
