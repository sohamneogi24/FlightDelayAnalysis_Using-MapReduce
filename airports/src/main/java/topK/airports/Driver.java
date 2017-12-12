package topK.airports;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO code application logic here

		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Chaining");
		job1.setJarByClass(Driver.class);
		
		job1.setMapperClass(TopFifteenMapper1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);

		job1.setReducerClass(TopFifteenReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		job1.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		//boolean complete = job1.waitForCompletion(true);
		
		boolean firstComplete = job1.waitForCompletion(true);
		boolean secondComplete=false;

		if (firstComplete) {
			Configuration conf2 = new Configuration();
			Job job2 = Job.getInstance(conf, "Chaining");
			job2.setJarByClass(Driver.class);
			job2.setMapperClass(TopFifteenMapper2.class);
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setSortComparatorClass(Comparator.class);
			job2.setReducerClass(TopFifteenReducer2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));

			secondComplete=job2.waitForCompletion(true);
		}
		
		Configuration conf3 = new Configuration();
		 Job job3 = Job.getInstance(conf3, "Join");
		 
		 
		 if(secondComplete){
			 
			 job3.setJarByClass(Driver.class); 
		     job3.setMapperClass(DelayRatingMapper.class);
		     job3.setMapperClass(AirportMapper.class);
		     job3.setMapOutputKeyClass(Text.class);
		     job3.setMapOutputValueClass(Text.class);
		     job3.setReducerClass(ReducerJoin.class);
		     
		     MultipleInputs.addInputPath(job3, new Path(args[2]), TextInputFormat.class, DelayRatingMapper.class);
		     MultipleInputs.addInputPath(job3, new Path(args[3]), TextInputFormat.class, AirportMapper.class);
		     job3.getConfiguration().set("join.type", "inner");
		     
		     
		     job3.setOutputKeyClass(Text.class);
		     job3.setOutputValueClass(Text.class);
		     
		             
		     FileInputFormat.addInputPath(job3, new Path(args[2]));
		     FileOutputFormat.setOutputPath(job3, new Path(args[4]));
			 
			 System.exit(job3.waitForCompletion(true)? 0 :1);
		 }

	}

}