import java.io.*;
import java.util.*;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;

public class SeqParse {
	static SequenceFile.Writer writer = null;
		
		private static void setKeyValue(FileSystem fileSystem,String filePath)throws Exception{
		File file = new File(filePath);
		File[] fs = file.listFiles();
		for(File f : fs){
			if(f.isDirectory()){
				setKeyValue(fileSystem,filePath+"/"+f.getName());
				}
			else{
				try{
					BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));

					String line = null;
					StringBuffer strBuffer = new StringBuffer();
					while((line = reader.readLine()) != null){
						strBuffer.append((String)line+" ");
						}
					Text key = new Text(filePath+"/"+f.getName());
					Text value = new Text(strBuffer.toString());
					
					writer.append(key,value);
					}
				catch(IOException e){
					e.printStackTrace();
					}
				}
			}

		}

	public static void main(String[] args) throws Exception{
		String uri = args[1];
		String filePath = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(filePath),conf);
		Path path = new Path(uri);
		writer = SequenceFile.createWriter(fs,conf,path,Text.class,Text.class);
		setKeyValue(fs,filePath);
		IOUtils.closeStream(writer);
    }
}


