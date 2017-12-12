package partitioning.weeklyAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	
	 public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
         Configuration conf = new Configuration();
         Job job = Job.getInstance(conf, "Weekly Partitioning count");
         job.setJarByClass(Driver.class);
         
         job.setMapperClass(WeeklyTrafficMapper.class);
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(IntWritable.class);
         
         job.setPartitionerClass(WeeklyPartitioner.class);
         job.setNumReduceTasks(7);
         
         job.setCombinerClass(WeeklyTrafficReducer.class);
         job.setReducerClass(WeeklyTrafficReducer.class);    
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(IntWritable.class);
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
