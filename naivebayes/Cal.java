import java.util.*;
import java.io.*;
public class Cal{
	public static void main(String[] args){
		BufferedReader reader = null; 
		Map<String,int[]> P = new HashMap<String,int[]>();
		Map<String,int[]> R = new HashMap<String,int[]>();
		double P1 = 0.0;
		double R1 = 0.0;
		double P2 = 0.0;
		double R2 = 0.0;
		int totalNumber = 0;
		int right = 0;
		try{
		File f = new File("part-00000");
	BufferedReader reader1 = new BufferedReader(new FileReader(new File("data/train/tmp/pct.txt")));
		String line1 = null;
		int total = 0;
		int classN = 0;
		while((line1 = reader1.readLine())!=null){
			total += Integer.parseInt(line1.split(" ")[1]);
			classN++;
			}
		System.out.println("total number of train file = "+total);
		System.out.println("total class of train file = "+classN);
	
		reader = new BufferedReader(new FileReader(f));
		String line = null;
		while((line = reader.readLine()) != null){
			String[] predict = (line.split("\t")[1]).split(" ");
			int[] tmp = new int[2];
			if(P.containsKey(predict[1])){
				tmp = P.get(predict[1]);
			}
			if(predict[0].equals(predict[1]))
				tmp[0]++;
			tmp[1]++;
		
			P.put(predict[1],tmp);
			int[]tmp2 = new int[2];
			if(R.containsKey(predict[0])){
				tmp2 = R.get(predict[0]);
			}
			if(predict[0].equals(predict[1]))
					tmp2[0]++;
			tmp2[1]++;
			
			//System.out.println(predict[0]+" "+tmp2[0]+" "+tmp2[1]);
			R.put(predict[0],tmp2);
			}
		System.out.println();
		}
		catch(Exception e){}
		finally{
			try{
			reader.close();
			}
			catch(Exception e){}
			}
		
		for(int[] p : P.values()){
			P1 += 1.0*p[0]/p[1];
			totalNumber += p[1];
			right += p[0];
			}
		P1 /= P.size();
		P2 = 1.0*right/totalNumber;
		System.out.println("right = "+right+"\ntotalNumber = "+totalNumber);
		totalNumber = 0;
		right = 0;
		for(int[] r : R.values()){
			R1 += 1.0*r[0]/r[1];
			totalNumber += r[1];
			right += r[0];
			}
		R1 /= R.size();
		R2 = 1.0*right/totalNumber;
		System.out.println("right2 = "+right+"\ntotalNumber2 = "+totalNumber);
		System.out.println("P1 = "+P1+"\nP2 = "+P2+"\nR1 = "+R1+"\nR2 = "+R2);
		System.out.println("F1 = "+2*P1*R1/(P1+R1)+"\n"+"F2 = "+2*P2*R2/(P2+R2));
		}
	}
