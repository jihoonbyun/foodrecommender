package foodRecommender;

import java.awt.Point;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import redis.clients.jedis.Jedis;


public class FoodOutputFormat extends OutputFormat<Text, IntWritable> {

	
	public static final String REDIS_HOSTS_CONF = "mapred.foodoutputformat.hosts";

	public static void setRedisHosts(Job job, String hosts) {
		job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
		System.out.println("	-	output path : "+ hosts);
	}


	public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		String csvHosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		return new RedisHashRecordWriter(csvHosts);
	}

	
	public void checkOutputSpecs(JobContext job) throws IOException {
		String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		if (hosts == null || hosts.isEmpty()) {
			throw new IOException(REDIS_HOSTS_CONF + " is not set in configuration.");
		}

	}

	
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
	}

	
	public static class RedisHashRecordWriter extends RecordWriter<Text, IntWritable> {

		String host;
		Jedis jedis;
		

		public RedisHashRecordWriter(String hosts) {
			this.host = hosts;
		}

		public void close(TaskAttemptContext context)
				throws IOException, InterruptedException {
		}

		public void write(Text key, IntWritable value) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			System.out.println("--------Reduce Write start");
			jedis = new Jedis(host, 6379);

				
				jedis.connect();
				jedis.select(0);
				jedis.set(key.toString(), value.toString());
				jedis.quit();
			
		}


	} // end 
} // end 