package foodRecommender;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.hsqldb.lib.HashMap;



public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>  {


	int [] maxtemp = new int[1000];
	int maxkey;
	int maxvalue = 0;
	int nextmaxkey = 0;

	Integer currentkey = 0;
	HashMap result = new HashMap();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

		System.out.println("----------------------------------------");
		int sum = 0;
		for(IntWritable v : values){
			sum += v.get();
			System.out.println("key :" + key);
			System.out.println("value :" + sum);			
		}
		
		String Key = key.toString();
		Integer row = Integer.parseInt(Key.substring(0,4));
		Integer column = Integer.parseInt(Key.substring(5,8));

		//dynamic programming
		if(currentkey == 0){
			maxtemp[column] = sum;
			maxvalue = sum;
			maxkey = column;
		
		}
		if(currentkey == row){
			
			if(sum > maxvalue){
				maxvalue =sum;
				maxkey = column;
			}
			
			if(sum > maxtemp[column]){
				maxtemp[column] = sum;
				nextmaxkey = row; 
			}
			
		}
		
		
		if(currentkey != row && currentkey != 0){
			System.out.println("currentkey : "  + currentkey);
			System.out.println("maxkey : "  + maxkey);			
			result.put(currentkey, maxkey);

			context.write(new Text(currentkey.toString()), new IntWritable(maxkey));
					
			maxvalue = maxtemp[row];
			if(sum > maxvalue){
				maxkey = column;
			}
			else{
				maxkey = nextmaxkey;
			}
		
		
		}
		currentkey = row;
		
	
		

		
	}
}
