package foodRecommender;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;




public class Map extends Mapper<LongWritable, Text, Text, IntWritable>  {
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {


		
		String line = value.toString().split(" ")[1];
		String[] productids = line.split(",");
		String newkey = null;
		for(int i=0; i < productids.length; i++){
			for(int j=0; j < productids.length; j++){
				if(j > i){
					if(Integer.parseInt(productids[i]) < Integer.parseInt(productids[j])){
						newkey = makeKeyFormat(Integer.parseInt(productids[i])) + makeKeyFormat(Integer.parseInt(productids[j]));
						//System.out.println(newkey);
					}
					else{
						newkey = makeKeyFormat(Integer.parseInt(productids[j])) + makeKeyFormat(Integer.parseInt(productids[i]));					
						//System.out.println(newkey);
					}
					System.out.println("key : " + newkey);
					context.write(new Text(newkey), new IntWritable(1));
				}
			}
		}
		//System.out.println("key : " + newkey);
		//context.write(new Text(newkey), new IntWritable(1));
		
	}

	private String makeKeyFormat(int productid){
		String key = "";
		if(productid >= 1000 && productid < 10000){
			key = Integer.toString(productid);
		}
		else if(productid >= 100 && productid < 1000){
			key = "0" + Integer.toString(productid);
		}
		else if(productid >= 10 && productid <100){
			key = "00" + Integer.toString(productid);
		}
		else if (productid >= 0 && productid < 10){
			key = "000" + Integer.toString(productid);
		}		
		return key;
	}



	}
	



