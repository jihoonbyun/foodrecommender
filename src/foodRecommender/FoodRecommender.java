package foodRecommender;



import foodRecommender.FoodOutputFormat;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class FoodRecommender {
	

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job();
		job = Job.getInstance(conf);
		job.setJobName("FOOD ANALYSIS START...");
		job.setJarByClass(FoodRecommender.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.out.println("[FOOD] INPUT: " + args[0]);
		//System.out.println("[FOOD] OUTPUT : " + args[1]);		
		
		//job.setInputFormatClass(FoodInputFormat.class);
		//job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(FoodOutputFormat.class);
		FoodOutputFormat.setRedisHosts(job, args[1]);

	
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	

			
		
	}

}
