package customWritable.cancellationAndDiversionRate;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CancellationDiversionMapper extends Mapper<Object, Text, Text, CustomWritable> {

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		CustomWritable cancelAndDivert = new CustomWritable();

		String[] row = value.toString().split(",");
		String carrier;
		if (!value.toString().contains("DayOfWeek")) {
			carrier = row[8];
			if (!row[21].equalsIgnoreCase("NA") && !row[23].equalsIgnoreCase("NA")) {
				cancelAndDivert.setCancellation(Float.parseFloat(row[21]));
				cancelAndDivert.setDiversion(Float.parseFloat(row[23]));
				context.write(new Text(carrier), cancelAndDivert);
			}
		}

	}
}