package partitioning.weeklyAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeeklyTrafficMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text dayOfTheWeek = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String row[] = value.toString().split(",");
		if (!value.toString().contains("DayOfWeek")) {
			if (!row[14].equalsIgnoreCase("NA")) {
				dayOfTheWeek.set(row[3]);
				context.write(dayOfTheWeek, new IntWritable(Integer.parseInt(row[14])));

			}
		}
	}

}
