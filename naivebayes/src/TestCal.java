import java.util.*;
import java.io.*;

class TestCal
{
	private static void calculate(String path)throws Exception{
		File file = new File(path);
		File[] fs = file.listFiles();
		int num = 0;
		for(File f : fs){
			if(f.isDirectory()){
				calculate(path+"/"+f.getName());
				}
			else{
				num++;
				}
			}
		if(num != 0){
			File f1 = new File("/root/text.txt");
			if(!f1.exists()){
				f1.createNewFile();
				}
			BufferedWriter writer = new BufferedWriter(new FileWriter(f1,true));
			writer.write(file.getName()+" "+num);
			writer.newLine();
			writer.flush();
			}

		}
	public static void main(String[] args)throws Exception{
		calculate("data/testdata");
		}
	}
