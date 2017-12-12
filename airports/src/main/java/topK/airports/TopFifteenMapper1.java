package topK.airports;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopFifteenMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		if (!value.toString().contains("UniqueCarrier")) {

			String row[] = value.toString().split(",");
			if (!row[14].equalsIgnoreCase("NA")) {

				String destAirport = row[17];

				// String arrDelay = row[14].trim();

				try {
					IntWritable busyRating = new IntWritable(Integer.parseInt(row[14]));
					context.write(new Text(destAirport), busyRating);

				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}

	}

}