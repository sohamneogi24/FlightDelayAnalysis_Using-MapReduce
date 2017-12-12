package speed.flightSpeed;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightSpeedMapper extends Mapper<Object, Text, Text, CustomWritable> {

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		CustomWritable flightSpeed = new CustomWritable();

		String[] row = value.toString().split(",");
		String carrier;
		if (!value.toString().contains("DayOfWeek")) {
			carrier = row[8];
			if (!row[18].equalsIgnoreCase("NA") && !row[13].equalsIgnoreCase("NA") && !row[18].equalsIgnoreCase("0") && !row[13].equalsIgnoreCase("0") ) {
				flightSpeed.setDistance(Float.parseFloat(row[18]));
				flightSpeed.setAirTime(Float.parseFloat(row[13]));
				
				context.write(new Text(carrier), flightSpeed);
			}
		}

	}
}