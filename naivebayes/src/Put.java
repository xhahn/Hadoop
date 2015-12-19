import java.util.*;
import java.io.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

public class Put{
	public static int sumOfDocs = 0;
	//combine all the small file to a large one 
	//the form of each record is "nameOfCalss.token"
	private void combine(String input,String output){
		File file = new File(input);
		File[] fs = file.listFiles();
		int numOfDocs = 0;
		for(File f : fs){
			if(f.isDirectory()&&f.getName().equals("tmp"))
				continue;
			else if(f.isDirectory())
				combine(input+"/"+f.getName(),output);
			else{
				numOfDocs++;
				File data = new File(output+"/trainData.txt");
				BufferedReader reader = null;
				BufferedWriter writer = null;
				try{
					if(!data.exists())
						data.createNewFile();
					reader = new BufferedReader(new FileReader(f));
					writer = new BufferedWriter(new FileWriter(data,true));
					String tempLine = null;
					while((tempLine = reader.readLine())!=null){
						writer.write(file.getName()+"."+tempLine);
						writer.newLine();
						}
					writer.flush();
					}
				catch(FileNotFoundException e){
					e.printStackTrace();
					}
				catch(IOException e){
					e.printStackTrace();
					}
				finally{
					try{
						writer.close();
						reader.close();
						}
					catch(IOException e){
						e.printStackTrace();	
						}
					}
			}
		}
		if(numOfDocs!=0){
			sumOfDocs += numOfDocs;
			File pc = new File(output+"/pct.txt");
			BufferedWriter writer = null;
			try{
				if(!pc.exists())
					pc.createNewFile();
				writer = new BufferedWriter(new FileWriter(pc,true));
				writer.write(file.getName()+" "+numOfDocs);
				writer.newLine();
				writer.flush();
				}
			catch(FileNotFoundException e){
				e.printStackTrace();
				}
			catch(IOException e){
				e.printStackTrace();
				}
			finally{
				try{
					writer.close();
					}
				catch(IOException e){
					e.printStackTrace();	
					}
				}
			}
	}

	private void calculatepc(String output){
			File pc = new File(output+"/pc.txt");
			File pct = new File(output+"/pct.txt");
			BufferedReader reader = null;
			BufferedWriter writer = null;
				try{
					if(!pc.exists())
						pc.createNewFile();
					reader = new BufferedReader(new FileReader(pct));
					writer = new BufferedWriter(new FileWriter(pc,true));
					String tempLine = null;
					while((tempLine = reader.readLine())!=null){
						String[] line = tempLine.split(" ");
						writer.write(line[0]+" "+Double.parseDouble(line[1])/sumOfDocs);
						writer.newLine();
						}
					writer.flush();
					}
				catch(FileNotFoundException e){
					e.printStackTrace();
					}
				catch(IOException e){
					e.printStackTrace();
					}
				finally{
					try{
						writer.close();
						reader.close();
						}
					catch(IOException e){
						e.printStackTrace();	
						}
					}
			}

		
	//put the train data into hadoop fs
	private void put(String[] input,String output)throws Exception{
		InputStream in = new BufferedInputStream(new FileInputStream(input[0]));
		
		InputStream in2 = new BufferedInputStream(new FileInputStream(input[1]));
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(output+"/trainData.txt"),conf);
		OutputStream out = fs.create(new Path(output+"/trainData.txt"));
		IOUtils.copyBytes(in,out,4096,true);
		FileSystem fs2 = FileSystem.get(URI.create(output+"/pc.txt"),conf);
		OutputStream out2 = fs.create(new Path(output+"/pc.txt"));
		IOUtils.copyBytes(in2,out2,4096,true);
		}

	public static void main(String[] args)throws Exception{
		if(args.length<2)
			System.err.println("more parameters are needed!");
		Put put = new Put();
		String tmp = args[0]+"/tmp";
		String[] input = {tmp+"/trainData.txt",tmp+"/pc.txt"};
		File file = new File(tmp);
		if(!file.exists())
			file.mkdir();
		put.combine(args[0],tmp);
		put.calculatepc(tmp);
		put.put(input,args[1]);
		}
}
