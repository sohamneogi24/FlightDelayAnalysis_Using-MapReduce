package partitioning.weeklyAnalysis;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Partitioner;


public class WeeklyPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numOfPartitions) {

		int week = Integer.parseInt(key.toString());

		return (week % numOfPartitions);
	}
}